from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path


def _write_csv(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def test_write_run_manifest_deterministic_and_sorted(tmp_path: Path) -> None:
    # Build a minimal fake run directory tree.
    run_dir = tmp_path / "runs" / "Remote_Code_Execution" / "20260101-000000-12345"

    # selection/selected.csv intentionally out of order to test sorting by Rank then URL.
    _write_csv(
        run_dir / "selection" / "selected.csv",
        """
Rank,URL,Title,Source_Domain,Category
2,https://example.com/b,Example B,example.com,Remote Code Execution
1,https://example.com/a,Example A,example.com,Remote Code Execution
1,https://example.com/aa,Example AA,example.com,Remote Code Execution
""",
    )

    # ranked.csv exists but is not required for ordering assertions.
    _write_csv(
        run_dir / "selection" / "ranked.csv",
        """
Rank,URL,Title,Source_Domain,Category
1,https://example.com/a,Example A,example.com,Remote Code Execution
2,https://example.com/b,Example B,example.com,Remote Code Execution
3,https://example.com/c,Example C,example.com,Remote Code Execution
""",
    )

    # scrape log keyed by URL
    _write_csv(
        run_dir / "scrape" / "scrape_log.csv",
        """
url,status,reason,cache,category,source_domain,title,artifact
https://example.com/a,ok,,hit,Remote Code Execution,example.com,Example A,
https://example.com/aa,fetch_fail,http_404,miss,Remote Code Execution,example.com,Example AA,
https://example.com/b,ok,,miss,Remote Code Execution,example.com,Example B,
""",
    )

    # export and docs meta
    _write_csv(
        run_dir / "exports" / "ctikg_input.csv",
        """
text,url,source_domain,title
hello,https://example.com/a,example.com,Example A
world,https://example.com/b,example.com,Example B
""",
    )

    (run_dir / "data").mkdir(parents=True, exist_ok=True)
    (run_dir / "data" / "ctikg_docs_meta.json").write_text(
        json.dumps([{"url": "https://example.com/a"}, {"url": "https://example.com/b"}], indent=2) + "\n",
        encoding="utf-8",
    )

    manifest_path = run_dir / "manifest.json"

    cmd = [
        sys.executable,
        str((Path(__file__).resolve().parents[1] / "scripts" / "write_run_manifest.py")),
        "--run-dir",
        str(run_dir),
        "--topic",
        "Remote Code Execution",
        "--provider",
        "dry-run",
        "--model",
        "ignored",
        "--scrape-max",
        "3",
        "--offset",
        "0",
        "--concurrency",
        "1",
        "--throttle-sec",
        "0",
        "--ignore-robots",
        "1",
        "--cache-enabled",
        "1",
        "--cache-db",
        ".cache/ctikg/scrape_cache.sqlite",
        "--cache-ttl-days",
        "30",
        "--rescue-enabled",
        "0",
        "--run-started-at-utc",
        "2026-01-01T00:00:00Z",
        "--run-finished-at-utc",
        "2026-01-01T00:00:10Z",
        "--dur-gen-yaml-sec",
        "1",
        "--dur-queue-sec",
        "2",
        "--dur-select-sec",
        "3",
        "--dur-scrape-sec",
        "4",
        "--dur-export-sec",
        "0",
        "--dur-verify-sec",
        "0",
        "--verify-status",
        "pass",
        "--out",
        str(manifest_path),
    ]

    # Run twice and assert the manifest is byte-for-byte identical (deterministic).
    subprocess.check_call(cmd, cwd=tmp_path)
    first = manifest_path.read_text(encoding="utf-8")

    subprocess.check_call(cmd, cwd=tmp_path)
    second = manifest_path.read_text(encoding="utf-8")

    assert first == second

    # The manifest is emitted with sort_keys=True. Enforce top-level key order for diff-friendliness.
    top_keys = []
    for line in first.splitlines():
        m0 = re.match(r'^  "([^"]+)":', line)
        if m0:
            top_keys.append(m0.group(1))
    assert top_keys == sorted(top_keys)

    m = json.loads(first)
    assert m["schema"] == "open-topic-run-manifest-v1"
    assert m["topic"] == "Remote Code Execution"

    # Schema presence + basic typing
    required_top = {
        "schema",
        "topic",
        "safe_topic",
        "run_id",
        "run_dir",
        "git",
        "inputs",
        "timing",
        "selection",
        "scrape",
        "exports",
        "verify",
    }
    assert required_top.issubset(set(m.keys()))
    assert isinstance(m["git"], dict)

    assert isinstance(m["inputs"], dict)
    assert isinstance(m["inputs"]["cache"]["enabled"], bool)
    assert isinstance(m["inputs"]["rescue"]["enabled"], bool)

    durations = m["timing"]["durations_sec"]
    for k in ("gen_yaml_sec", "queue_sec", "select_sec", "scrape_sec", "export_sec", "verify_sec"):
        assert k in durations

    # URLs should be sorted by Rank then URL.
    assert m["selection"]["selected_urls"] == [
        "https://example.com/a",
        "https://example.com/aa",
        "https://example.com/b",
    ]
    assert m["selection"]["selected_total"] == len(m["selection"]["selected_urls"])

    # attempted list should follow the same order.
    attempted_urls = [x["url"] for x in m["scrape"]["attempted"]]
    assert attempted_urls == m["selection"]["selected_urls"]
    assert m["scrape"]["attempted_total"] == len(m["scrape"]["attempted"])
    assert m["scrape"]["ok_total"] == len(m["scrape"]["ok_urls"])
    for item in m["scrape"]["attempted"]:
        assert {"url", "status", "reason", "cache"}.issubset(set(item.keys()))

    # Basic row counts
    assert m["exports"]["ctikg_input_rows"] == 2
    assert m["exports"]["docs_meta"]["type"] == "list"
    assert m["exports"]["docs_meta"]["count"] == 2
    assert m["verify"]["status"] == "pass"

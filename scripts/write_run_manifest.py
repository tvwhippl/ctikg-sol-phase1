#!/usr/bin/env python3
"""Write deterministic per-run manifest JSON for v1 `make open-topic`.

Manifest path:
  runs/<SAFE_TOPIC>/<RUN_ID>/manifest.json

Design goals:
  - deterministic JSON (sorted keys)
  - stable ordering of selected URLs (by Rank then URL)
  - captures: inputs, selected URLs, scrape outcomes, timestamps/durations, verify status, export row counts
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import urlsplit, urlunsplit


def canonical_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return u
    try:
        parts = urlsplit(u)
        scheme = (parts.scheme or "http").lower()
        netloc = (parts.netloc or "").lower()
        return urlunsplit((scheme, netloc, parts.path or "", parts.query or "", ""))
    except Exception:
        return u


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _read_csv_dicts(path: Path) -> List[Dict[str, str]]:
    if not path.is_file() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        rdr = csv.DictReader(f)
        return [dict(r) for r in rdr if r]


def _read_selected_urls(selected_csv: Path) -> Tuple[int, List[str]]:
    rows = _read_csv_dicts(selected_csv)
    items: List[Tuple[int, str]] = []
    for r in rows:
        url = canonical_url(r.get("URL") or r.get("url") or "")
        if not url:
            continue
        rank = _safe_int(r.get("Rank") or r.get("rank") or 0, 0)
        items.append((rank, url))
    items.sort(key=lambda t: (t[0], t[1]))
    return len(rows), [u for _, u in items]


def _count_csv_rows(csv_path: Path) -> int:
    if not csv_path.is_file() or csv_path.stat().st_size == 0:
        return 0
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        n = sum(1 for _ in f)
    return max(0, n - 1)


def _git_info() -> Dict[str, Any]:
    info: Dict[str, Any] = {}
    try:
        info["commit"] = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        info["dirty"] = bool(subprocess.check_output(["git", "status", "--porcelain"], text=True).strip())
    except Exception:
        pass
    return info


def main() -> None:
    ap = argparse.ArgumentParser(description="Write v1 open-topic run manifest")
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--topic", required=True)

    ap.add_argument("--provider", default="")
    ap.add_argument("--model", default="")

    ap.add_argument("--scrape-max", type=int, default=0)
    ap.add_argument("--offset", type=int, default=0)
    ap.add_argument("--concurrency", type=int, default=0)
    ap.add_argument("--throttle-sec", type=float, default=0.0)
    ap.add_argument("--ignore-robots", type=int, default=0)

    ap.add_argument("--cache-enabled", type=int, default=0)
    ap.add_argument("--cache-db", default="")
    ap.add_argument("--cache-ttl-days", type=float, default=0.0)

    ap.add_argument("--rescue-enabled", type=int, default=0)
    ap.add_argument("--rescue-max-add", type=int, default=0)
    ap.add_argument("--rescue-min-qsim", type=float, default=0.10)

    ap.add_argument("--run-started-at-utc", default="")
    ap.add_argument("--run-finished-at-utc", default="")

    ap.add_argument("--dur-gen-yaml-sec", type=int, default=-1)
    ap.add_argument("--dur-queue-sec", type=int, default=-1)
    ap.add_argument("--dur-select-sec", type=int, default=-1)
    ap.add_argument("--dur-scrape-sec", type=int, default=-1)
    ap.add_argument("--dur-export-sec", type=int, default=-1)
    ap.add_argument("--dur-verify-sec", type=int, default=-1)

    ap.add_argument("--verify-status", default="unknown", choices=["pass", "fail", "unknown"])
    ap.add_argument("--out", default=None)

    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    out_path = Path(args.out) if args.out else (run_dir / "manifest.json")

    ranked_csv = run_dir / "selection" / "ranked.csv"
    selected_csv = run_dir / "selection" / "selected.csv"
    scrape_log_csv = run_dir / "scrape" / "scrape_log.csv"
    scrape_stats_json = run_dir / "scrape" / "scrape_stats.json"
    export_csv = run_dir / "exports" / "ctikg_input.csv"
    docs_meta_json = run_dir / "data" / "ctikg_docs_meta.json"

    ranked_total = len(_read_csv_dicts(ranked_csv))
    selected_total, selected_urls = _read_selected_urls(selected_csv)

    scrape_rows = _read_csv_dicts(scrape_log_csv)
    scrape_by_url: Dict[str, Dict[str, str]] = {}
    for r in scrape_rows:
        u = canonical_url(r.get("url") or r.get("URL") or "")
        if not u:
            continue
        scrape_by_url[u] = r

    attempted: List[Dict[str, Any]] = []
    ok_urls: List[str] = []
    for u in selected_urls:
        r = scrape_by_url.get(u, {})
        status = (r.get("status") or "").strip()
        reason = (r.get("reason") or "").strip()
        cache = (r.get("cache") or "").strip()
        attempted.append({"url": u, "status": status, "reason": reason, "cache": cache})
        if status == "ok":
            ok_urls.append(u)

    stats: Dict[str, Any] = {}
    if scrape_stats_json.is_file():
        try:
            stats = json.loads(scrape_stats_json.read_text(encoding="utf-8"))
        except Exception:
            stats = {}

    export_rows = _count_csv_rows(export_csv)
    docs_meta_summary: Dict[str, Any] = {}
    if docs_meta_json.is_file():
        try:
            raw = json.loads(docs_meta_json.read_text(encoding="utf-8"))
            if isinstance(raw, list):
                docs_meta_summary = {"type": "list", "count": len(raw)}
            elif isinstance(raw, dict):
                # common shapes: {"docs": [...]} or arbitrary dict
                if isinstance(raw.get("docs"), list):
                    docs_meta_summary = {"type": "dict", "count": len(raw.get("docs"))}
                elif isinstance(raw.get("documents"), list):
                    docs_meta_summary = {"type": "dict", "count": len(raw.get("documents"))}
                else:
                    docs_meta_summary = {"type": "dict", "count": len(raw)}
            else:
                docs_meta_summary = {"type": type(raw).__name__, "count": 0}
        except Exception:
            docs_meta_summary = {}

    safe_topic = run_dir.parent.name if run_dir.parent else ""
    run_id = run_dir.name

    manifest: Dict[str, Any] = {
        "schema": "open-topic-run-manifest-v1",
        "topic": str(args.topic),
        "safe_topic": safe_topic,
        "run_id": run_id,
        "run_dir": str(run_dir),
        "git": _git_info(),
        "inputs": {
            "provider": str(args.provider),
            "model": str(args.model),
            "scrape_max": _safe_int(args.scrape_max, 0),
            "offset": _safe_int(args.offset, 0),
            "concurrency": _safe_int(args.concurrency, 0),
            "throttle_sec": _safe_float(args.throttle_sec, 0.0),
            "ignore_robots": _safe_int(args.ignore_robots, 0),
            "cache": {
                "enabled": bool(_safe_int(args.cache_enabled, 0)),
                "db": str(args.cache_db),
                "ttl_days": _safe_float(args.cache_ttl_days, 0.0),
            },
            "rescue": {
                "enabled": bool(_safe_int(args.rescue_enabled, 0)),
                "max_add": _safe_int(args.rescue_max_add, 0),
                "min_qsim": _safe_float(args.rescue_min_qsim, 0.10),
            },
        },
        "timing": {
            "run_started_at_utc": str(args.run_started_at_utc),
            "run_finished_at_utc": str(args.run_finished_at_utc),
            "durations_sec": {
                "gen_yaml_sec": _safe_int(args.dur_gen_yaml_sec, -1),
                "queue_sec": _safe_int(args.dur_queue_sec, -1),
                "select_sec": _safe_int(args.dur_select_sec, -1),
                "scrape_sec": _safe_int(args.dur_scrape_sec, -1),
                "export_sec": _safe_int(args.dur_export_sec, -1),
                "verify_sec": _safe_int(args.dur_verify_sec, -1),
            },
        },
        "selection": {
            "ranked_total": int(ranked_total),
            "selected_total": int(selected_total),
            "selected_urls": selected_urls,
        },
        "scrape": {
            "attempted_total": int(len(attempted)),
            "ok_total": int(len(ok_urls)),
            "ok_urls": ok_urls,
            "attempted": attempted,
            "stats": stats,
        },
        "exports": {
            "ctikg_input_rows": int(export_rows),
            "docs_meta": docs_meta_summary,
        },
        "verify": {
            "status": str(args.verify_status),
        },
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()

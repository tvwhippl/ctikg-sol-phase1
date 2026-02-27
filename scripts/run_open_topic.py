#!/usr/bin/env python3

"""Run the open-topic pipeline for ONE topic YAML into an isolated run directory.

Why this exists:
  - The original Makefile targets write to fixed paths (results/, exports/, data/)
    and rely on "latest" generated YAMLs. That is convenient for interactive use,
    but it collides under multi-topic batch runs and HPC job arrays.

This runner:
  - takes an explicit topic YAML
  - writes per-topic outputs under --run-dir
  - optionally enables semantic fill (low precision) to reach `winners`

It is intentionally thin glue around existing scripts.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict

import re
import yaml


def _safe_name(name: str) -> str:
    """Filesystem-safe identifier."""
    s = re.sub(r"[^A-Za-z0-9]+", "_", (name or "").strip())
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "Topic"


def _run(cmd: list[str]) -> None:
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if p.returncode != 0:
        sys.stderr.write(p.stdout)
        raise SystemExit(p.returncode)
    # Stream output on success too (helps in slurm logs)
    sys.stdout.write(p.stdout)


def main() -> None:
    ap = argparse.ArgumentParser(description="Run one open-topic YAML in an isolated run directory")
    ap.add_argument("--topic-yaml", required=True, help="Path to topic YAML (name/include/exclude/winners)")
    ap.add_argument(
        "--queue",
        default="data/Links_Queue_sorted_flags.csv",
        help="Input link queue CSV (default: data/Links_Queue_sorted_flags.csv)",
    )
    ap.add_argument(
        "--run-dir",
        default=None,
        help="Output directory for this topic run (default: runs/<SAFE_TOPIC>/<timestamp>)",
    )

    # Selection controls
    ap.add_argument("--fill-to-winners", action="store_true", help="Pad selection to winners using semantic similarity")
    ap.add_argument("--min-qsim", type=float, default=0.0, help="Minimum TF-IDF similarity for fallback/fill")

    # Scrape controls
    ap.add_argument("--max-per-category", type=int, default=None, help="Max rows to scrape (default: YAML winners)")
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--throttle-sec", type=float, default=0.0)
    ap.add_argument("--ignore-robots", action="store_true")

    # Ops
    ap.add_argument("--resume", action="store_true", help="Skip steps whose outputs already exist")
    ap.add_argument("--no-scrape", action="store_true", help="Stop after selection (debug)")
    ap.add_argument("--no-export", action="store_true", help="Stop after scrape (debug)")

    args = ap.parse_args()

    topic_yaml = Path(args.topic_yaml)
    if not topic_yaml.is_file():
        raise SystemExit(f"Missing topic YAML: {topic_yaml}")

    cat: Dict[str, Any] = yaml.safe_load(topic_yaml.read_text(encoding="utf-8"))
    if not isinstance(cat, dict) or not cat.get("name"):
        raise SystemExit(f"Invalid topic YAML (expected mapping with name/include/exclude/winners): {topic_yaml}")

    topic_name = str(cat.get("name") or "Topic").strip() or "Topic"
    safe = _safe_name(topic_name)

    ts = time.strftime("%Y%m%d-%H%M%S")
    run_dir = Path(args.run_dir) if args.run_dir else Path("runs") / safe / ts
    run_dir.mkdir(parents=True, exist_ok=True)

    # Standard per-run layout
    d_data = run_dir / "data"
    d_results = run_dir / "results"
    d_exports = run_dir / "exports"
    d_artifacts = run_dir / "artifacts"
    for d in (d_data, d_results, d_exports, d_artifacts):
        d.mkdir(parents=True, exist_ok=True)

    # Copy YAML for provenance
    run_yaml = run_dir / f"{safe}.yaml"
    if not run_yaml.exists():
        run_yaml.write_text(topic_yaml.read_text(encoding="utf-8"), encoding="utf-8")

    selected_csv = d_data / f"Selected_{safe}.csv"
    scrape_log = d_results / "scrape_log.csv"
    scraped_jsonl = d_results / "scraped_corpus.jsonl"
    out_csv = d_exports / "ctikg_input.csv"
    docs_meta = d_data / "ctikg_docs_meta.json"

    # ---------- 1) Select ----------
    if not (args.resume and selected_csv.is_file() and selected_csv.stat().st_size > 0):
        cmd = [
            sys.executable,
            "scripts/category_select.py",
            "--in",
            str(args.queue),
            "--category",
            str(run_yaml),
            "--out",
            str(selected_csv),
            "--min-qsim",
            str(float(args.min_qsim)),
        ]
        if args.fill_to_winners:
            cmd.append("--fill-to-winners")
        _run(cmd)
        if not selected_csv.is_file() or selected_csv.stat().st_size == 0:
            raise SystemExit(f"Selection produced empty output: {selected_csv}")
    else:
        print(f"[resume] selection exists: {selected_csv}")

    if args.no_scrape:
        return

    # Determine scrape cap
    winners = int(cat.get("winners") or 25)
    max_per_category = int(args.max_per_category) if args.max_per_category is not None else winners

    # ---------- 2) Scrape ----------
    if not (args.resume and scraped_jsonl.is_file() and scraped_jsonl.stat().st_size > 0):
        cmd = [
            sys.executable,
            "scripts/scrape_selected.py",
            "--in",
            str(selected_csv),
            "--out",
            str(scrape_log),
            "--jsonl",
            str(scraped_jsonl),
            "--artifacts",
            str(d_artifacts),
            "--max_per_category",
            str(max_per_category),
            "--concurrency",
            str(int(args.concurrency)),
            "--throttle_sec",
            str(float(args.throttle_sec)),
        ]
        if args.ignore_robots:
            cmd.append("--ignore_robots")
        _run(cmd)
    else:
        print(f"[resume] corpus exists: {scraped_jsonl}")

    if args.no_export:
        return

    # ---------- 3) Export ----------
    if not (args.resume and out_csv.is_file() and out_csv.stat().st_size > 0):
        _run(
            [
                sys.executable,
                "scripts/export_ctikg_input.py",
                "--in_jsonl",
                str(scraped_jsonl),
                "--out_csv",
                str(out_csv),
                "--out_docs",
                str(docs_meta),
                "--log_csv",
                str(scrape_log),
            ]
        )
    else:
        print(f"[resume] export exists: {out_csv}")

    # ---------- 4) Verify ----------
    _run([sys.executable, "scripts/verify_export.py", "--corpus", str(scraped_jsonl), "--csv", str(out_csv)])

    # ---------- 5) Run metadata ----------
    meta = {
        "topic": topic_name,
        "safe": safe,
        "topic_yaml": str(run_yaml),
        "queue": str(args.queue),
        "selected_csv": str(selected_csv),
        "scrape_log": str(scrape_log),
        "scraped_jsonl": str(scraped_jsonl),
        "export_csv": str(out_csv),
        "docs_meta": str(docs_meta),
        "fill_to_winners": bool(args.fill_to_winners),
        "min_qsim": float(args.min_qsim),
        "max_per_category": int(max_per_category),
    }
    (run_dir / "run_meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"[OK] run_dir={run_dir}")


if __name__ == "__main__":
    main()

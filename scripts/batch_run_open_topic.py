#!/usr/bin/env python3

"""Batch runner for MANY topic YAMLs.

This is a thin wrapper around scripts/run_open_topic.py that:
  - finds topic YAMLs in a directory (or reads a list file)
  - runs them sequentially

For HPC, prefer a job array that invokes run_open_topic.py per topic.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

import yaml

import re


def _safe_name(name: str) -> str:
    s = re.sub(r"[^A-Za-z0-9]+", "_", (name or "").strip())
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "Topic"


def _run(cmd: list[str]) -> int:
    p = subprocess.run(cmd)
    return int(p.returncode)


def main() -> None:
    ap = argparse.ArgumentParser(description="Run many open-topic YAMLs")
    ap.add_argument("--topics-dir", default=None, help="Directory containing *.yaml topic configs")
    ap.add_argument("--topics-list", default=None, help="Text file with one YAML path per line")
    ap.add_argument("--queue", default="data/Links_Queue_sorted_flags.csv")
    ap.add_argument("--runs-root", default="runs", help="Root directory for per-topic run outputs")
    ap.add_argument(
        "--batch-id",
        default=None,
        help="Optional batch identifier. Default: YYYYMMDD-HHMMSS. Run dirs become <runs-root>/<batch-id>/<topic>/...",
    )
    ap.add_argument("--resume", action="store_true")

    # Pass-through knobs
    ap.add_argument("--fill-to-winners", action="store_true")
    ap.add_argument("--min-qsim", type=float, default=0.0)
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--throttle-sec", type=float, default=0.0)
    ap.add_argument("--ignore-robots", action="store_true")
    args = ap.parse_args()

    yamls: list[Path] = []
    if args.topics_list:
        p = Path(args.topics_list)
        yamls = [Path(line.strip()) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]
    elif args.topics_dir:
        d = Path(args.topics_dir)
        yamls = sorted(d.glob("*.yaml")) + sorted(d.glob("*.yml"))
    else:
        raise SystemExit("Provide --topics-dir or --topics-list")

    if not yamls:
        raise SystemExit("No topic YAMLs found")

    failures = 0
    batch_id = args.batch_id or time.strftime("%Y%m%d-%H%M%S")
    run_root = Path(args.runs_root) / batch_id
    run_root.mkdir(parents=True, exist_ok=True)

    for y in yamls:
        # Determine SAFE topic name from YAML
        try:
            cat = yaml.safe_load(y.read_text(encoding="utf-8"))
        except Exception as e:
            failures += 1
            print(f"[FAIL] {y} could not parse YAML: {e}")
            continue
        if not isinstance(cat, dict) or not cat.get("name"):
            failures += 1
            print(f"[FAIL] {y} missing required key: name")
            continue
        safe = _safe_name(str(cat.get("name")))
        run_dir = run_root / safe

        cmd = [
            sys.executable,
            "scripts/run_open_topic.py",
            "--topic-yaml",
            str(y),
            "--queue",
            str(args.queue),
            "--run-dir",
            str(run_dir),
            "--concurrency",
            str(int(args.concurrency)),
            "--throttle-sec",
            str(float(args.throttle_sec)),
            "--min-qsim",
            str(float(args.min_qsim)),
        ]
        if args.fill_to_winners:
            cmd.append("--fill-to-winners")
        if args.ignore_robots:
            cmd.append("--ignore-robots")
        if args.resume:
            cmd.append("--resume")

        print(f"\n=== {y} ===")
        rc = _run(cmd)
        if rc != 0:
            failures += 1
            print(f"[FAIL] {y} rc={rc}")

    if failures:
        raise SystemExit(f"Batch finished with failures={failures}/{len(yamls)}")
    print(f"[OK] Batch finished: {len(yamls)} topics")


if __name__ == "__main__":
    main()

# Phase-1 Smoke Test (phase1 / topic pipeline)

## Purpose
A fast, deterministic smoke test that verifies the end-to-end Phase-1 pipeline (topic-pull -> merge -> select -> small scrape -> export) before promoting to SOL or running large jobs. Designed to catch the "no rows" failure early.

## Files
- `tests/smoke_phase1.sh` — executable smoke test script.
- `open_topic.mk` — Makefile target `topic-pull` now contains the fail-fast guard.
- `scripts/merge_dedup.py` — accepts `--no-clobber-batch` to avoid overwrite.

## Run locally
1. Activate your virtualenv:
   ```bash
   source .venv/bin/activate

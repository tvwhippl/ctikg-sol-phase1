Summary
-------
This PR:
- Adds a fail-fast guard to open_topic.mk to abort the pipeline if batch_topic.csv contains only a header (or is otherwise empty).
- Adds a --no-clobber-batch arg and fixes argparse initialization in scripts/merge_dedup.py (prevents accidental overwrites).
- Adds tests/smoke_phase1.sh — a reproducible smoke test that runs topic-pull, topic-select, and topic-scrape (limited), and verifies outputs.
- Adds docs/SMOKE_TEST.md documenting how to run the smoke test locally and on SOL.

Why
---
Prevents wasted downstream work and false successes when the link-pull step returns no new items. The smoke test provides fast, repeatable verification before pushing to SOL.

Files changed
-------------
- open_topic.mk
- scripts/merge_dedup.py
- tests/smoke_phase1.sh
- docs/SMOKE_TEST.md
- (optional) CHANGELOG.md

Testing
-------
Run locally:
1. Activate venv
2. `chmod +x tests/smoke_phase1.sh`
3. `./tests/smoke_phase1.sh`
4. Confirm `SMOKE TEST PASSED` and exit code 0.

Notes
-----
- CI: I recommend adding a lightweight GitHub Actions job `smoke.yml` that runs the smoke test on `push` to the feature branch (see suggested workflow in PR comments).
- SOL run will be done in a new thread/PR.

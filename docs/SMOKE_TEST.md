# Smoke tests

Smoke tests are fast, end-to-end checks that catch the most common failure mode:

- “The pipeline ran, but produced no usable export rows.”

They are meant to be run locally before pushing changes or kicking off larger runs.

---

## v1 open-topic smoke test (recommended)

This exercises the **one-command** interface:

- `make open-topic ...` → scrape a tiny sample → export → verify

It uses `PROVIDER=dry-run` so **topic YAML generation does not call an LLM**.

### Run

```bash
source .venv/bin/activate
bash tests/smoke_open_topic.sh
```

### Pass criteria

- Script prints `SMOKE OPEN-TOPIC PASSED: runs/...`
- The run contains:
  - `runs/.../exports/ctikg_input.csv` (non-empty)
  - `runs/.../data/ctikg_docs_meta.json` (non-empty)

---

## Legacy phase-1 smoke test

This exercises the older multi-step Make flow (`topic-gen/topic-pull/topic-select/topic-scrape/topic-chunk`).

### Run

```bash
source .venv/bin/activate
bash tests/smoke_phase1.sh
```

---

## Notes

- Smoke tests still hit the network for link retrieval and scraping.
- If a smoke test fails due to scraping denials/timeouts, reduce `CONCURRENCY`, increase `THROTTLE_SEC`, and re-run.

# Pipeline details

This document explains each stage, the inputs/outputs, and knobs you can tune.

```
topics → queue → triage → selection → scrape → export → verify
```

## v1 open-topic (one command)

The canonical Goal-1 path is:

```bash
make open-topic TOPIC="..." PROVIDER=openai MODEL="..." SCRAPE_MAX=50
```

It writes all artifacts into an isolated per-run directory:

- `runs/<SAFE_TOPIC>/<RUN_ID>/{config,queue,selection,scrape,artifacts,exports,data}`

Immediate run outputs are:

- `runs/.../scrape/scraped_corpus.jsonl`
- `runs/.../exports/ctikg_input.csv`
- `runs/.../data/ctikg_docs_meta.json`
- `runs/.../selection/ranked.csv`
- `runs/.../selection/selected.csv`
- `runs/.../selection/selection_summary.json`

The open-topic selector now enforces a quality gate for semantic fallback.
By default, semantic fallback requires `QuerySim >= 0.005`, exclude-only fallback is off,
and topic YAML may further restrict fallback with `fallback_anchors` and
`fallback_anchor_min_hits`. Each run writes `selection/selection_summary.json`
so the operator can see whether the topic filled, underfilled, or stopped after
the quality gate.

Primary notebook handoff is a manual post-run export step:

- `python scripts/export_llm4cti_articles.py --run-dir runs/<SAFE_TOPIC>/<RUN_ID>`
- `runs/.../llm4cti/Articles.xlsx`
- `runs/.../llm4cti/llm4cti_articles.csv`
- `runs/.../llm4cti/llm4cti_articles_meta.json`

Verify a run later with:

```bash
make verify RUN_DIR="runs/<SAFE_TOPIC>/<RUN_ID>"
```

For output interpretation and success criteria, see `docs/OUTPUTS_CONTRACT.md`.

For the canonical SOL staged-input and ranked-offset array pattern, see `docs/SOL_RUNBOOK.md`.

---

## Topics

- Author by hand or generate with `gen_category_from_llm.py` (Ollama).
- Generated files live under `configs/categories/_generated/*.yaml`.
- `make topic-setup` scaffolds directories & defaults.
- `make topic-gen TOPIC="..."` writes a YAML topic set with `include/exclude` sections.

## Link queue (`topic-pull`)

- `make topic-pull SOURCES=configs/sources/common.json`:
  - reads your selected categories,
  - pulls links from configured sources,
  - deduplicates with `scripts/merge_dedup.py`,
  - writes `data/Links_Queue.csv` and `batch_topic.csv` (for bookkeeping).

## Triage helpers

- `python scripts/make_helper_flags.py [in_path]`:
  - normalizes column names (supports both `url/URL`, `title/Title`, etc.),
  - computes quick flags (`RepFlag`, `SigFlag`, `Quality2/4`),
  - writes `data/Links_Queue_sorted_flags.csv`,
  - can also write optional runtime triage packs for manual review.
- These runtime triage files can help you eyeball the queue and adjust the category set if needed.

## Selection (`topic-select`)

- Chooses top N per category into `data/Selected_*.csv`.
- Control with `WINNERS=<N>` when calling `make topic-select` (or the combined `topic-scrape`).

## Scrape (`topic-scrape`)

- Reads `data/Selected_*.csv` and fetches pages in parallel.
- Key arguments surfaced by `make`:
  - `CONCURRENCY` → number of fetch workers,
  - `THROTTLE_SEC` → polite sleep between completed fetches,
  - `IGNORE_ROBOTS` → if `1`, passes `--ignore_robots` to the scraper.
- Outputs:
  - `results/scrape_log.csv` (status, reason, category, source_domain, title, artifact),
  - `results/scraped_corpus.jsonl` (only rows with `status=="ok"` and non‑empty `text`).

## Export

- `scripts/export_ctikg_input.py`:
  - input: `results/scraped_corpus.jsonl`,
  - output: `exports/ctikg_input.csv`, `data/ctikg_docs_meta.json`,
  - schema: `sentence,url,category,title,source_domain`.

- `scripts/verify_export.py` asserts the CSV exists and has >0 rows.

## Optional: Chunking

- `scripts/chunk_articles.py` can pre‑split long documents for downstream CTIKG runs.

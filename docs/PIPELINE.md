# Pipeline details

This document explains each stage, the inputs/outputs, and knobs you can tune.

```
topics → queue → triage → selection → scrape → export → verify
```

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
  - writes per‑category packs `Triage_*_top200.csv` and `Suggested_Selected_master.csv`.
- These files help you eyeball the queue and adjust the category set if needed.

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

# LLM4CTI Notebook Bridge

## Primary handoff
The primary handoff from this repo into the current notebook-based LLM4CTI workflow is:

- `scrape/scraped_corpus.jsonl`
- `scripts/export_llm4cti_articles.py`
- `llm4cti/Articles.xlsx`
- `llm4cti/llm4cti_articles.csv`
- `llm4cti/llm4cti_articles_meta.json`

## Why
The current LLM4CTI notebook is article-first. It expects article content that can be chunked inside the notebook.

Because of that, `exports/ctikg_input.csv` is useful but secondary.

## Status of other paths
- `scripts/run_simple_ctikg.py` is an optional smoke-test adapter.
- `exports/ctikg_input.csv` is a secondary or legacy export.
- The preferred notebook bridge is article-level export from `scraped_corpus.jsonl`.

## Example
Run:

`python scripts/export_llm4cti_articles.py --run-dir <RUN_DIR>`

This writes notebook-ready files into:

`<RUN_DIR>/llm4cti/`

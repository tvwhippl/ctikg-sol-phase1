# Simple CTIKG Integration (optional adapter)

This document describes the optional adapter `scripts/run_simple_ctikg.py`.

It is useful for quick smoke tests, but it is not the primary handoff path for the current notebook-based LLM4CTI workflow.

Primary notebook bridge:
- `docs/LLM4CTI_NOTEBOOK_BRIDGE.md`
- `scripts/export_llm4cti_articles.py`

This adapter converts Phase-1 pipeline output `results/scraped_corpus.jsonl` to a simplified single-prompt CTIKG-style run.

## Quickstart

1. Activate your venv:

```bash
source .venv/bin/activate
```

2. Run the Phase-1 smoke test (or run the pipeline manually until results/scraped_corpus.jsonl exists):

```bash
./tests/smoke_phase1.sh
```

3. Run Simple CTIKG adapter directly:

```bash
python scripts/run_simple_ctikg.py --input results/scraped_corpus.jsonl --output outputs/simple_ctikg --max-docs 10
```

4. Open the visualization:

```bash
open outputs/simple_ctikg/simple_ctikg_graph.html
```

## Notes

* Default LLM provider is OpenRouter. Set `OPENROUTER_API_KEY` and `OPENROUTER_BASE_URL` env vars, or pass `--openrouter-api-key` explicitly.
* Use `--dry-run` to prepare input and skip LLM calls.
* The adapter saves `outputs/simple_ctikg/input_docs.json` for auditability.

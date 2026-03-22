# Outputs Contract

## Purpose

This document defines what outputs this repo produces, which outputs are primary versus secondary, and what an operator should treat as:

- immediate run outputs
- manual post-run handoff outputs
- audit/debug artifacts
- legacy shared-path outputs

This repo is a **CTI article acquisition and packaging front-end**.

It is not the final downstream graph-extraction package.

## Contract boundary

The pipeline’s job is to:

- acquire and rank topic-relevant CTI article candidates
- scrape a bounded selected batch
- export verified run artifacts
- provide a clean handoff into downstream CTIKG / LLM4CTI-style workflows

The pipeline does not claim to:

- perform the final official downstream packaged graph-extraction workflow
- solve topic-ranking robustness for every topic
- guarantee the same viable batch size for every topic

## Primary output families

### 1) Open-topic per-run outputs

The recommended workflow is:

```bash
make open-topic TOPIC="..." PROVIDER=... MODEL=... SCRAPE_MAX=...
```

This creates an isolated run directory:

- `runs/<SAFE_TOPIC>/<RUN_ID>/`

Within that run directory, the primary immediate outputs are:

- `config/topic.yaml`
- `queue/Links_Queue.csv`
- `queue/Links_Queue_sorted_flags.csv`
- `selection/ranked.csv`
- `selection/selected.csv`
- `scrape/scraped_corpus.jsonl`
- `scrape/scrape_log.csv`
- `scrape/scrape_stats.json`
- `exports/ctikg_input.csv`
- `data/ctikg_docs_meta.json`
- `manifest.json`

Interpretation:
- `scraped_corpus.jsonl` is the highest-fidelity article payload produced directly by the scraper
- `ctikg_input.csv` is the verified sentence-level export
- `ctikg_docs_meta.json` is document-level metadata aligned to the export
- `manifest.json` is the run-level provenance record

### 2) Manual post-run article-first handoff outputs

These are not created automatically by `make open-topic`.

Create them explicitly with:

```bash
python scripts/export_llm4cti_articles.py --run-dir runs/<SAFE_TOPIC>/<RUN_ID>
```

This writes:

- `llm4cti/Articles.xlsx`
- `llm4cti/llm4cti_articles.csv`
- `llm4cti/llm4cti_articles_meta.json`

Interpretation:
- these are the preferred handoff artifacts for the current notebook-based LLM4CTI workflow
- article-first handoff is primary for the current notebook path
- `ctikg_input.csv` remains useful as a secondary or compatibility-oriented export

### 3) SOL ranked-offset array outputs

For the canonical SOL pattern, each array shard writes an isolated run directory:

- `runs/<SAFE_TOPIC>/slurm-<JOBID>/offset_<OFFSET>/`

Each shard should contain the same core output families as a normal per-run path:

- staged copies of topic/queue/ranked inputs needed for audit
- `selection/selected.csv`
- `scrape/scraped_corpus.jsonl`
- `scrape/scrape_log.csv`
- `scrape/scrape_stats.json`
- `exports/ctikg_input.csv`
- `data/ctikg_docs_meta.json`
- `manifest.json`

The article-first `llm4cti/` files remain a manual post-run export step.

## Which outputs are primary

### Primary for current downstream notebook handoff
- `scrape/scraped_corpus.jsonl`
- `llm4cti/Articles.xlsx`
- `llm4cti/llm4cti_articles.csv`
- `llm4cti/llm4cti_articles_meta.json`

### Primary for sentence-level compatibility / inspection
- `exports/ctikg_input.csv`
- `data/ctikg_docs_meta.json`

### Primary for provenance / audit
- `manifest.json`
- `config/topic.yaml`
- `selection/ranked.csv`
- `selection/selected.csv`
- `scrape/scrape_log.csv`
- `scrape/scrape_stats.json`

## Secondary and optional outputs

### Optional downstream compatibility proof outputs

If you intentionally run the compatibility proof path, additional downstream-style outputs may be produced, such as:

- `article_kg_raw.csv`
- `graph_nodes.csv`
- `graph_edges.csv`
- `graph.gexf`
- `summary.json`

Interpretation:
- these are compatibility-proof artifacts
- they demonstrate that article-level handoff can feed a downstream graph-style extraction workflow
- they are not the official final packaged downstream pipeline

### Legacy shared-path outputs

Older Make targets may still write shared-path outputs such as:

- `results/scraped_corpus.jsonl`
- `results/scrape_log.csv`
- `exports/ctikg_input.csv`
- `data/ctikg_docs_meta.json`

Interpretation:
- these legacy outputs are still usable
- they are not the preferred path for new runs
- shared-path outputs can collide across repeated runs, which is why the per-run `runs/<SAFE_TOPIC>/<RUN_ID>/` path is preferred

## Minimum successful run contract

A run is minimally successful when all of the following are true:

- `selection/selected.csv` contains rows
- `scrape/scraped_corpus.jsonl` exists and is non-empty
- `exports/ctikg_input.csv` exists
- `data/ctikg_docs_meta.json` exists
- `manifest.json` exists
- the export passes `scripts/verify_export.py`

A run may still be acceptable when:

- `selected.csv` contains fewer rows than the requested `SCRAPE_MAX`
- the ranked pool is smaller than hoped but still relevant
- `llm4cti/` files are absent because the manual article-export step has not been run yet

A run is not complete for current notebook handoff until the article-first `llm4cti/` files have been created explicitly.

## Failure and no-op interpretation

Interpret outputs this way:

- empty `selection/selected.csv` in the normal open-topic path is a run failure
- an array shard whose offset is beyond the ranked pool is a clean no-op, not a failure
- missing `llm4cti/` files alone do not mean the scrape/export failed
- underfilled selection is a topic/ranking limitation unless logs show an actual execution fault
- shard failures should be diagnosed from Slurm logs, `sacct`, and run artifacts rather than inferred from a single missing output

## Storage and git rules

Generated outputs should not be committed to git.

Do not commit:
- run directories under `runs/`
- scraped corpora
- export CSV/XLSX outputs
- tarballs
- evidence bundles
- Slurm logs captured for a specific run

Do preserve representative evidence off-repo for handoff and validation.

Commit to git:
- scripts
- tests
- operator-facing documentation
- small example configs, if needed

## Related docs

- `README.md`
- `docs/index.md`
- `docs/OPEN_TOPIC_QUICKSTART.md`
- `docs/SOL_RUNBOOK.md`
- `docs/PIPELINE.md`
- `docs/LLM4CTI_NOTEBOOK_BRIDGE.md`
- `docs/LLM4CTI_COMPAT_TEST.md`
- `docs/TROUBLESHOOTING.md`

# Handoff Index

## Current repo state
- Branch: `main`
- Current pushed commit: `7d4fff9`
- Recent key commits:
  - `7d4fff9` Add LLM4CTI downstream compatibility runner
  - `99592e8` Add LLM4CTI notebook bridge export and docs
  - `45800b7` Fix malformed expanded sources config JSON

## Primary docs
- `README.md`
- `docs/OPEN_TOPIC_QUICKSTART.md`
- `docs/LLM4CTI_NOTEBOOK_BRIDGE.md`
- `docs/LLM4CTI_COMPAT_TEST.md`
- `docs/SIMPLE_CTIKG_INTEGRATION.md`

## Primary scripts
- `scripts/export_llm4cti_articles.py`
- `scripts/run_llm4cti_compat.py`

## Canonical handoff model
Primary downstream handoff is article-first:
- `scrape/scraped_corpus.jsonl`
- `llm4cti/Articles.xlsx`
- `llm4cti/llm4cti_articles.csv`
- `llm4cti/llm4cti_articles_meta.json`

Secondary outputs:
- `exports/ctikg_input.csv`
- `scripts/run_simple_ctikg.py` (optional adapter)

## Representative sample package
Persistent handoff tarball:
- `/home/tvwhippl/ctikg_handoff_20260323_remote_code_execution.tgz`

Expanded handoff directory:
- `/home/tvwhippl/ctikg_handoff_20260323/Remote_Code_Execution_sample/`

## Included evidence in sample package
Run artifacts:
- `manifest.json`
- `topic.yaml`
- `scraped_corpus.jsonl`
- `scrape_log.csv`
- `scrape_stats.json`
- `ctikg_input.csv`
- `ctikg_docs_meta.json`
- `Articles.xlsx`
- `llm4cti_articles.csv`
- `llm4cti_articles_meta.json`

Downstream compatibility artifacts:
- `article_kg_raw.csv`
- `graph_nodes.csv`
- `graph_edges.csv`
- `graph.gexf`
- `summary.json`

Repo-state artifacts:
- `head_commit.txt`
- `recent_commits.txt`
- `git_status.txt`

## Important interpretation
- Open-topic ingestion on SOL is operational.
- Article-first notebook handoff is now the preferred downstream interface.
- The compatibility runner is a valid downstream proof, but not the final official packaged Liangyi pipeline.

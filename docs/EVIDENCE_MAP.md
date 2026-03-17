# Evidence Map

## Claim 1
The open-topic ingestion front-end runs on SOL with the correct architecture.

Evidence:
- topic generation on login node
- staged inputs under `runs/_stage/...`
- run directories with `manifest.json`, scrape logs/stats, and exports
- representative sample package:
  - `/home/tvwhippl/ctikg_handoff_20260323/Remote_Code_Execution_sample/`

## Claim 2
A real repo defect was found and fixed.

Evidence:
- commit `45800b7` Fix malformed expanded sources config JSON

## Claim 3
Large topic-size targets were not a reliable success metric.

Evidence:
- Topic 1 scale attempts showed ranking collapse even after expanded-source repair
- this prompted a pivot away from arbitrary 500/1000 per-topic targets

## Claim 4
The project handoff is now aligned to the current notebook-based LLM4CTI workflow.

Evidence:
- commit `99592e8` Add LLM4CTI notebook bridge export and docs
- `docs/LLM4CTI_NOTEBOOK_BRIDGE.md`
- `scripts/export_llm4cti_articles.py`

## Claim 5
A downstream compatibility test succeeded on SOL.

Evidence:
- commit `7d4fff9` Add LLM4CTI downstream compatibility runner
- output summary:
  - articles processed: 2
  - nodes: 10
  - edges: 8
- saved outputs:
  - `article_kg_raw.csv`
  - `graph_nodes.csv`
  - `graph_edges.csv`
  - `graph.gexf`
  - `summary.json`

## Claim 6
A persistent handoff artifact exists for transfer and review.

Evidence:
- `/home/tvwhippl/ctikg_handoff_20260323_remote_code_execution.tgz`

## Presentation cautions
Do not claim:
- that Topic 1 achieved true large-scale success
- that the compatibility runner is the final official Liangyi pipeline

Do claim:
- the SOL ingest path was operationalized
- the repo was corrected and documented
- the handoff boundary was improved to article-first export
- downstream compatibility was proven on a real sample

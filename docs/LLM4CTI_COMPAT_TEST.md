# LLM4CTI Compatibility Test

This document describes the downstream compatibility runner used to validate that the Phase-1 article export can be consumed by an LLM4CTI-style graph extraction workflow on SOL.

## Purpose

This is not the final Liangyi notebook replacement.

It is a compatibility proof that demonstrates:

- `llm4cti/Articles.xlsx` can be consumed directly
- Voyager/OpenAI-compatible inference works on SOL
- graph-style entity/relationship output can be produced and saved
- the handoff from open-topic ingestion into downstream graph extraction is real

## Script

- `scripts/run_llm4cti_compat.py`

## Inputs

- `Articles.xlsx` produced by `scripts/export_llm4cti_articles.py`

## Outputs

- `article_kg_raw.csv`
- `graph_nodes.csv`
- `graph_edges.csv`
- `graph.gexf`
- `summary.json`

## Interpretation

Use this as a downstream compatibility test and presentation artifact.

Do not present it as the final official LLM4CTI packaged pipeline.

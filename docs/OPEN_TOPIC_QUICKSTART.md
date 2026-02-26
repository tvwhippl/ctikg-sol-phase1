# Open Topic Pipeline Quickstart

This repo supports an “Open Topic” workflow: given a human topic (e.g., “Remote Code Execution”), generate a topic config via an LLM, select winners from an article queue, scrape the winners, chunk them into CTI-KG input rows, and verify the output.

## What “Open Topic” is (and isn’t)

**It is:** a way to generate a topic configuration YAML:
- `name`: canonical topic name used for downstream filenames
- `include`: keyword phrases to identify relevant items
- `exclude`: keyword phrases to filter obvious off-topic items
- `winners`: target number of selected articles

**It is not:** the CTI-KG extraction step. This pipeline produces `exports/ctikg_input.csv`, which is an input for later CTI-KG / LLM4CTIKG processing.

## What the pipeline does

1. **topic-gen**: LLM generates a topic YAML config:
   - `name`
   - `include` (8–15 short keyword phrases)
   - `exclude` (5–12 phrases)
   - `winners` (int)
2. **topic-select**: selects “winner” rows from `data/queue.csv` into `data/Selected_<Topic>.csv`
3. **topic-scrape**: scrapes the selected URLs into `results/scraped_corpus.jsonl`
4. **topic-chunk**: chunks/splits scraped text and exports a sentence CSV to `exports/ctikg_input.csv`
5. **topic-verify**: sanity checks the export

## Outputs / Artifacts

Given TOPIC="Remote Code Execution", the pipeline generates:

- Topic YAML:
  - `configs/categories/_generated/Remote_Code_Execution.yaml`
- Winners CSV:
  - `data/Selected_Remote_Code_Execution.csv`
- Scrape logs + corpus:
  - `results/scrape_log.csv`
  - `results/scraped_corpus.jsonl`
- CTI-KG input:
  - `exports/ctikg_input.csv`
  - `data/ctikg_docs_meta.json`

## Prereqs

- Python venv created and dependencies installed.
- `make` available.
- A populated `data/queue.csv` (your candidate URL queue)
- Optional for local LLM:
  - Ollama installed and running.

## LLM Providers

The topic-generation step supports multiple providers via `scripts/gen_category_from_llm.py`:

- `--provider openai` (OpenAI-compatible, includes ASU Voyager proxy)
- `--provider ollama` (local)
- `--provider dry-run` (no network; returns a canned config)

### Provider: ASU Voyager (recommended for project work)

1) Request an API key via Voyager UI (see ASU RC docs).
2) Choose a model from Voyager Model Directory (example: `llama4-scout-17b`).
3) Export env vars:

```bash
export LLM_PROVIDER=openai
export OPENAI_BASE_URL="https://openai.rc.asu.edu/v1"
export OPENAI_API_KEY="YOUR_VOYAGER_KEY"
export LLM_MODEL="llama4-scout-17b"

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
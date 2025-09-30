# Open-topic pipeline — quickstart & ops (macOS/Linux + Ollama)

This guide explains how to run the open-topic pipeline end-to-end to produce:
- `exports/ctikg_input.csv` — final CSV for ingestion
- `data/ctikg_docs_meta.json` — metadata for the exported docs

It supports **macOS Apple Silicon** and **Linux** using **Ollama** + **Llama 3.1**.

---

## Prerequisites

- Python 3.10+ and a virtualenv: `.venv` (already in this repo)
- **Ollama** installed and the model pulled:
  - macOS: `brew install ollama`
  - Linux: `curl -fsSL https://ollama.com/install.sh | sh`
  - Model: `ollama pull llama3.1`
- Activate env:
  ```bash
  source .venv/bin/activate
  export LLM_PROVIDER=ollama
  export LLM_MODEL=llama3.1

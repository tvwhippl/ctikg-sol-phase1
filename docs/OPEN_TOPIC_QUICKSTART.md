# Open-topic pipeline — Quickstart & Ops (macOS/Linux + Ollama)

This pipeline lets you generate a topic-specific category YAML with an LLM (Ollama),
pull/prioritize links, scrape winners, chunk text, and export CTIKG inputs.

> Tested on macOS with Python 3.13 and Ollama (Meta Llama 3.1).

---

## Prerequisites

- Python 3.11+ (3.13 works great)
- `pip install -r requirements.txt`
- [Ollama](https://ollama.com) installed and running locally
- Pull a model once: `ollama list | grep -i llama3.1 || ollama pull llama3.1`

Optional environment (defaults shown):
```bash
export LLM_PROVIDER=ollama
export LLM_MODEL=llama3.1

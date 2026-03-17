## LLM4CTI handoff

For the current notebook-based LLM4CTI workflow, the preferred downstream bridge is **article-level export**, not just sentence-level CSV.

Primary notebook bridge:
- `docs/LLM4CTI_NOTEBOOK_BRIDGE.md`
- `scripts/export_llm4cti_articles.py`

Secondary paths:
- `exports/ctikg_input.csv` for sentence-level inspection / compatibility
- `docs/SIMPLE_CTIKG_INTEGRATION.md` for the optional single-prompt adapter

# ctikg-sol-phase1

**Phase‑1 pipeline** to select topic‑focused CTI articles, scrape + triage them, and export inputs for CTIKG experiments.  
Supports **Ollama** (local) and **OpenAI‑compatible** LLM endpoints (e.g., ASU Voyager).

> Status: verified end‑to‑end (Ollama + OpenAI‑compatible backends) — see docs for exact env vars.

---

## Why this exists

This repo provides a compact, reproducible pipeline for **topic selection → link curation → scraping → export**.  
It is designed to feed downstream CTIKG/graph extraction experiments and small, auditable datasets.

---

## TL;DR (Quick start)

```bash
# 0) clone & enter
git clone https://github.com/tvwhippl/ctikg-sol-phase1.git
cd ctikg-sol-phase1

# 1) Python env
python3 -m venv .venv && source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt

# 2) LLM provider

## Option A: Ollama (local LLM) – start + pull a model
# (macOS) ensure service is up and model exists
brew services start ollama
ollama pull llama3.1:8b
export LLM_PROVIDER=ollama
export LLM_MODEL=llama3.1:8b
export LLM_BASE_URL="http://127.0.0.1:11434"

## Option B: OpenAI-compatible (e.g., ASU Voyager)

export LLM_PROVIDER=openai
export OPENAI_BASE_URL="https://openai.rc.asu.edu/v1"
export OPENAI_API_KEY="YOUR_KEY"
export LLM_MODEL="llama4-scout-17b"   # example; pick from the provider's model directory

# 3) One-command open-topic (recommended)
make open-topic \
  TOPIC="NFS File Share Exposure" \
  PROVIDER="$LLM_PROVIDER" MODEL="$LLM_MODEL" \
  SCRAPE_MAX=25 CONCURRENCY=4 THROTTLE_SEC=1 IGNORE_ROBOTS=1

# The command prints: [OK] run_dir=runs/<SAFE_TOPIC>/<RUN_ID>
# You can re-verify a specific run:
# make verify RUN_DIR="runs/<SAFE_TOPIC>/<RUN_ID>"

# 3b) Legacy multi-step pipeline (writes to shared paths under data/ results/ exports/ artifacts/)
make topic-setup
make topic-gen TOPIC="CI/CD pipeline attacks: runner poisoning, OIDC misconfiguration, artifact/cache poisoning"
make topic-pull  SOURCES=configs/sources/common.json
make topic-select

# 4) Scrape (legacy)
# tune WINNERS/CONCURRENCY/THROTTLE_SEC/IGNORE_ROBOTS as needed
make topic-scrape WINNERS=25 CONCURRENCY=4 THROTTLE_SEC=1 IGNORE_ROBOTS=1

# 5) Export & verify dataset (legacy)
python scripts/export_ctikg_input.py --in_jsonl results/scraped_corpus.jsonl --out_csv exports/ctikg_input.csv --out_docs data/ctikg_docs_meta.json
python scripts/verify_export.py
```

Outputs to check:

- (open-topic) `runs/.../exports/ctikg_input.csv` (sentences + url + category + title + source_domain)
- (open-topic) `runs/.../data/ctikg_docs_meta.json` (doc-level metadata)
- (open-topic) `runs/.../scrape/scrape_log.csv` (success/error and reasons)
- (legacy) `exports/ctikg_input.csv`, `data/ctikg_docs_meta.json`, `results/scrape_log.csv`
- optional triage packs in repo root (e.g., `Triage_*_top200.csv`)

For a step‑by‑step walkthrough and what each target does, see:

- **[docs/OPEN_TOPIC_QUICKSTART.md](docs/OPEN_TOPIC_QUICKSTART.md)** (open-topic pipeline; Ollama + Voyager)
- **[docs/QUICKSTART.md](docs/QUICKSTART.md)** and **[docs/PIPELINE.md](docs/PIPELINE.md)** (phase‑1 pipeline background)

---

## Repo layout (selected)

```
configs/
  categories/         # curated and generated topic category sets
  sources/            # source lists
data/
  Links_Queue.csv     # merged link queue per topic selection
  ctikg_docs_meta.json
docs/                 # user docs for GitHub
exports/
  ctikg_input.csv     # final phase‑1 export
results/
  scrape_log.csv
  scraped_corpus.jsonl
scripts/
  chunk_articles.py
  export_ctikg_input.py
  gen_category_from_llm.py
  make_helper_flags.py
  merge_dedup.py
  pre_rank_links_v3.py
  scrape_selected.py
  verify_export.py
open_topic.mk         # make targets for the topic workflow
Makefile
```

---

## Ollama notes

- Set once per shell:
  ```bash
  export LLM_PROVIDER=ollama
  export LLM_MODEL=llama3.1:8b
  export LLM_BASE_URL="http://127.0.0.1:11434"
  ```
- Sanity check the local API:
  ```bash
  curl -s http://127.0.0.1:11434/api/tags | jq . | head
  ```

More details and recovery steps are in **[docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md)**.

---

## What “scrape” uses and how to be polite

The scraper supports two knobs that are surfaced by `make topic-scrape`:

- `IGNORE_ROBOTS` → flips `--ignore_robots` in `scripts/scrape_selected.py` (default off).
- `THROTTLE_SEC` → maps to `--throttle_sec` (sleep in seconds between completed fetches).

> Recommendation: keep `THROTTLE_SEC` ≥ 1 for public sources. Only set `IGNORE_ROBOTS=1` for your own sites or when you have explicit permission.

---

## Typical data flow

1) **Topics** (manual or LLM‑assisted) → `configs/categories/_generated/*.yaml`  
2) **Link queue** (`make topic-pull` + `merge_dedup.py`) → `data/Links_Queue.csv`  
3) **Flags/Triage** (`make_helper_flags.py`) → `data/Links_Queue_sorted_flags.csv`, `Triage_*_top200.csv`  
4) **Winners** (`make topic-select`) → `data/Selected_*.csv`  
5) **Scrape** (`make topic-scrape`) → `results/scraped_corpus.jsonl` (+ `results/scrape_log.csv`)  
6) **Export** (`export_ctikg_input.py` + `verify_export.py`) → `exports/ctikg_input.csv`, `data/ctikg_docs_meta.json`

The mechanics and file contracts are documented in **[docs/PIPELINE.md](docs/PIPELINE.md)**.

---

## Generating good topics

You can author topics by hand or synthesize category lists via `gen_category_from_llm.py` (Ollama).  
See **[docs/TOPICS.md](docs/TOPICS.md)** for concrete patterns, anti‑patterns, and examples that work well with this pipeline.

---

## Troubleshooting (fast)

- **Ollama “server not responding”** → `brew services restart ollama`, then `ollama pull llama3.1:8b` and `curl` test.  
- **`exports/ctikg_input.csv` is empty** → check `results/scrape_log.csv` for error counts; if many timeouts or denials, lower concurrency, set `THROTTLE_SEC=1..2`, and, if you own the sources, `IGNORE_ROBOTS=1`.  
- **BrokenPipeError when piping `-h` output** → harmless (comes from shell pipeline closing early). Re‑run the command directly without `| sed ...` to view help.

More: **[docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md)**.

---

## References

- CTIKG: LLM‑powered knowledge graph construction from CTI (COLM 2024).
- Internal LLM4CTI notes on chunking + long/short‑term memory design.

---

## License & attribution

This repo includes custom scripts authored for the SOL/CTIKG Phase‑1 workflow.  
Cite upstream sources and respect website terms of service when scraping.

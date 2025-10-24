# Quick start

The short path from a fresh clone to a verified CSV export.

## 0) Clone and set up Python

```bash
git clone https://github.com/tvwhippl/ctikg-sol-phase1.git
cd ctikg-sol-phase1
python3 -m venv .venv && source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## 1) Start Ollama & pull a model (local LLM)

```bash
brew services start ollama     # macOS service
ollama pull llama3.1:8b
export LLM_PROVIDER=ollama
export LLM_MODEL=llama3.1:8b
export LLM_BASE_URL="http://127.0.0.1:11434"
# optional sanity check
curl -s http://127.0.0.1:11434/api/tags | jq . | head
```

## 2) Generate/select topics

```bash
make topic-setup

# Option A: write your own topic list (see docs/TOPICS.md)

# Option B: synthesize a topic set with LLM
make topic-gen TOPIC="CI/CD pipeline attacks: runner poisoning, OIDC misconfiguration, artifact/cache poisoning"

# Pull+merge sources and build the link queue
make topic-pull  SOURCES=configs/sources/common.json

# Select winners into data/Selected_*.csv
make topic-select
```

## 3) Scrape

```bash
# knobs: WINNERS (max rows), CONCURRENCY, THROTTLE_SEC, IGNORE_ROBOTS
make topic-scrape WINNERS=25 CONCURRENCY=4 THROTTLE_SEC=1 IGNORE_ROBOTS=1
```

Artifacts:
- `results/scrape_log.csv` – success/error log.
- `results/scraped_corpus.jsonl` – JSONL corpus (url, title, text, category, source_domain).

## 4) Export + verify

```bash
python scripts/export_ctikg_input.py --in_jsonl results/scraped_corpus.jsonl   --out_csv exports/ctikg_input.csv --out_docs data/ctikg_docs_meta.json

python scripts/verify_export.py
```

Success checklist:
- `exports/ctikg_input.csv` has rows.
- `head -n 3 exports/ctikg_input.csv` shows columns: `sentence,url,category,title,source_domain`.

Done. Proceed to chunking or downstream CTIKG steps as needed.

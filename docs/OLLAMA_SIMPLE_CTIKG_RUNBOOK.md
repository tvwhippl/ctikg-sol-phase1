# Ollama + Simple CTIKG — Quick Runbook

Purpose
-------
This document captures the **exact, verified steps** to run the Phase‑1 pipeline output through **Simple CTIKG using Ollama**.
It exists so the same process can be reproduced later on SOL without rediscovery or guesswork.

This runbook validates **pipeline → LLM → structured output** integration.
It does *not* claim high extraction recall — that is the role of the full CTIKG system.

Location
--------
docs/OLLAMA_SIMPLE_CTIKG_RUNBOOK.md

Prerequisites
-------------
- Clean checkout of this repository (recommended branch: `chore/add-scraper-deps`)
- Python 3.10+
- Virtual environment available at `.venv`
- `ollama` installed and available on PATH
- Do **not** commit runtime outputs or secrets

Environment Variables
---------------------
Required:
- `OLLAMA_HOST`  
  Example:
  ```bash
  export OLLAMA_HOST="http://127.0.0.1:11434"
  ```

Optional:
- `CTIKG_MODEL` (default handled by runner fallback)
  ```bash
  export CTIKG_MODEL="llama3.2:latest"
  ```

Never commit:
- `OPENROUTER_API_KEY`
- Any API credentials

Local Setup (Copy/Paste)
------------------------

### 1. Activate virtual environment
```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

Install required libraries (minimum):
```bash
python -m pip install requests tqdm json_repair
```

### 2. Start Ollama
```bash
pkill -f ollama || true
ollama start &> /tmp/ollama_serve_debug.log & disown
sleep 2
tail -n 40 /tmp/ollama_serve_debug.log
```

Confirm service and models:
```bash
curl -sS "$OLLAMA_HOST/v1/models" | jq .
```

If needed:
```bash
ollama pull llama3.2:latest
```

Pipeline Preconditions
----------------------
The following artifacts **must already exist** before running Simple CTIKG:

```bash
exports/ctikg_input.csv
data/ctikg_docs_meta.json
```

They are produced by:
```bash
make topic-setup
make topic-pull
make topic-select
make topic-scrape
make topic-chunk
```

Simple CTIKG — Smoke Test
-------------------------
Run a small test first (10 docs):

```bash
head -n 10 exports/ctikg_input.csv > exports/ctikg_input_small.csv

python scripts/run_simple_ctikg.py \
  --input exports/ctikg_input_small.csv \
  --out results/simple_ctikg_results_small.jsonl \
  --model "${CTIKG_MODEL:-llama3.2:latest}"
```

Inspect output:
```bash
sed -n '1,5p' results/simple_ctikg_results_small.jsonl
```

Expected:
- One JSON object per line
- Each line contains `doc_id` and `result`
- Some documents may contain empty or missing triples (expected behavior)

Simple CTIKG — Full Run
----------------------

### macOS (prevent sleep)
```bash
caffeinate -dimsu \
python scripts/run_simple_ctikg.py \
  --input exports/ctikg_input.csv \
  --out results/simple_ctikg_results.jsonl \
  --model "${CTIKG_MODEL:-llama3.2:latest}" \
  2>&1 | tee /tmp/simple_ctikg_run.log
```

### SOL / HPC
- Use `tmux` or `screen`
- Ensure Ollama is running on the compute node
- Capture the bound IP and export `OLLAMA_HOST`
- Ensure model is pulled on the node (`ollama pull …`)

Verification Checks
-------------------

Basic:
```bash
wc -l results/simple_ctikg_results.jsonl
grep -c '"triples"' results/simple_ctikg_results.jsonl
grep -c '"error"' results/simple_ctikg_results.jsonl
```

Distribution:
```bash
python - <<'PY'
import json
from collections import Counter

counts = Counter()
bad = 0
with open("results/simple_ctikg_results.jsonl") as fh:
    for line in fh:
        j = json.loads(line)
        r = j.get("result",{})
        if isinstance(r, dict) and isinstance(r.get("triples"), list):
            counts[len(r["triples"])] += 1
        else:
            bad += 1

print("Bad / no-triples:", bad)
print("Triple count distribution:", dict(counts))
PY
```

Visualization (Fallback)
------------------------
PyVis has known template issues on some Python versions.
Use the fallback visualizer:

```bash
python scripts/visualize_ctikg_fallback.py \
  --input results/simple_ctikg_results.jsonl \
  --out results/simple_ctikg_viz_fallback.html \
  --top 150
```

Open the HTML file locally in a browser.

Known Behavior & Limitations
----------------------------
- Simple CTIKG uses **one LLM call per document**
- It is intentionally conservative
- Short / low-signal CTI feed items often produce no triples
- This is expected and confirms non-hallucinating behavior
- High recall requires the **full CTIKG** system

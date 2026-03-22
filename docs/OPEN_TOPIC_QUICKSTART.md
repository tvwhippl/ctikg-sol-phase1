# Open Topic Pipeline Quickstart

This is the recommended local or single-run workflow for this repo.

Use it when you want:

- one topic string
- one isolated run directory
- verified export artifacts
- optional article-first downstream handoff after the run

For output interpretation and success criteria, see `docs/OUTPUTS_CONTRACT.md`.

For the canonical SOL staged-input and ranked-offset array pattern, see `docs/SOL_RUNBOOK.md`.

This repo supports an **open topic** workflow.

## v1 one-command open-topic (recommended)

If you want **one command** from a topic string → verified CTIKG inputs (Goal 1), run:

```bash
make open-topic \
  TOPIC="Remote Code Execution" \
  PROVIDER=openai MODEL="llama4-scout-17b" \
  SCRAPE_MAX=25 OFFSET=0 CONCURRENCY=4 THROTTLE_SEC=1 IGNORE_ROBOTS=1
```

This writes an isolated run directory:

- `runs/<SAFE_TOPIC>/<RUN_ID>/...`

Key outputs immediately after a run completes:

Immediate outputs:
- `runs/.../scrape/scraped_corpus.jsonl`
- `runs/.../exports/ctikg_input.csv`
- `runs/.../data/ctikg_docs_meta.json`

Manual post-run notebook handoff (after running `scripts/export_llm4cti_articles.py`):
- `runs/.../llm4cti/Articles.xlsx`
- `runs/.../llm4cti/llm4cti_articles.csv`
- `runs/.../llm4cti/llm4cti_articles_meta.json`

Create notebook handoff files explicitly after the run:

`python scripts/export_llm4cti_articles.py --run-dir runs/<SAFE_TOPIC>/<RUN_ID>`

For output interpretation and success criteria, see `docs/OUTPUTS_CONTRACT.md`.

For the canonical SOL operating pattern, staged inputs, and ranked-offset array usage, see `docs/SOL_RUNBOOK.md`.

Re-verify a run later:

```bash
make verify RUN_DIR="runs/<SAFE_TOPIC>/<RUN_ID>"
```

### Pagination with `OFFSET`

Selection outputs (for debugging + pagination):

- `runs/.../selection/ranked.csv` (full ranked candidates; not capped by `SCRAPE_MAX`)
- `runs/.../selection/selected.csv` (slice used for scraping: `OFFSET .. OFFSET+SCRAPE_MAX`)

Example: scrape 3 at a time (page 0 then page 1):

```bash
make open-topic TOPIC="Remote Code Execution" PROVIDER=dry-run MODEL=ignored \
  SCRAPE_MAX=3 OFFSET=0 CONCURRENCY=1 THROTTLE_SEC=1 IGNORE_ROBOTS=1

make open-topic TOPIC="Remote Code Execution" PROVIDER=dry-run MODEL=ignored \
  SCRAPE_MAX=3 OFFSET=3 CONCURRENCY=1 THROTTLE_SEC=1 IGNORE_ROBOTS=1
```

Note: the topic YAML field `winners` is metadata in the v1 path; the operational cap is `SCRAPE_MAX`.

### Scrape caching (URL-based, safe-by-default for v1 open-topic)

The v1 `make open-topic` pipeline enables a small shared URL→text cache by default so repeated runs (and pagination pages) don’t re-scrape identical URLs unnecessarily.

- Default cache DB: `.cache/ctikg/scrape_cache.sqlite` (safe to delete)
- Disable caching: `SCRAPE_CACHE=0`
- Override location: `SCRAPE_CACHE_DB=path/to/cache.sqlite`
- TTL: `SCRAPE_CACHE_TTL_DAYS=30` (set to `0` to disable expiration)

Cache signals:

- per-URL lines like: `[cache] HIT url=...`
- summary line: `[cache] summary hits=<N> misses=<N> db=<path>`

### Bounded rescue fill (opt-in, only when underfilled)

Underfill is OK by default; the selector will **not** pad low-quality links.

If you want to attempt a bounded semantic rescue *only when the selected slice underfills `SCRAPE_MAX`*, set:

- `RESCUE=1`
- `RESCUE_MAX_ADD` (max additional candidates appended; bounded)
- `RESCUE_MIN_QSIM` (minimum semantic similarity threshold for rescue candidates)

Example:

```bash
make open-topic TOPIC="Remote Code Execution" PROVIDER=dry-run MODEL=ignored \
  SCRAPE_MAX=25 OFFSET=0 CONCURRENCY=2 THROTTLE_SEC=1 IGNORE_ROBOTS=1 \
  RESCUE=1 RESCUE_MAX_ADD=50 RESCUE_MIN_QSIM=0.15
```

Rescue is fully logged and never runs unless the page is underfilled.

### Run manifest (auditability)

Each v1 run writes a deterministic JSON manifest:

- `runs/<SAFE_TOPIC>/<RUN_ID>/manifest.json`

It captures:

- input parameters (topic, provider/model, `SCRAPE_MAX`/`OFFSET`, concurrency/throttle, ignore_robots)
- selected URLs and scraped URLs
- timestamps and step durations
- verify status + export row counts


---

## Legacy multi-step pipeline

The legacy Make targets are still supported, but they write to shared fixed paths (`data/`, `results/`, `exports/`, `artifacts/`) and will collide under batch runs.

Legacy flow:

`topic-gen → topic-pull → topic-select → topic-scrape → topic-chunk → topic-verify`

## Prereqs

- Python 3.10+ and `make`
- A Python venv with dependencies installed
- A populated link queue (typically built via `make topic-pull`, see below)

### Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

---

## LLM providers

Topic generation (`topic-gen`) uses `scripts/gen_category_from_llm.py` and supports:

- **Ollama** (`LLM_PROVIDER=ollama`) for local models
- **OpenAI-compatible** (`LLM_PROVIDER=openai`) including **ASU Voyager**
- **dry-run** (`LLM_PROVIDER=dry-run`) for offline smoke tests

### Provider: Ollama (local)

```bash
brew services start ollama              # macOS
ollama pull llama3.1:8b

export LLM_PROVIDER=ollama
export LLM_MODEL=llama3.1:8b
export LLM_BASE_URL="http://127.0.0.1:11434"
```

### Provider: ASU Voyager (OpenAI-compatible)

1) Create / copy your Voyager API key from the Voyager UI.
2) Choose a model from the Voyager Model Directory.
3) Export env vars:

```bash
export LLM_PROVIDER=openai
export OPENAI_BASE_URL="https://openai.rc.asu.edu/v1"
export OPENAI_API_KEY="YOUR_VOYAGER_KEY"
export LLM_MODEL="llama4-scout-17b"     # example
```

Sanity check (optional):

```bash
curl -s "$OPENAI_BASE_URL/chat/completions" \
  -H "Authorization: Bearer $OPENAI_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"model":"'"$LLM_MODEL"'","messages":[{"role":"user","content":"ping"}]}' \
  | head
```

---

## Single-topic run with Make

Example topic:

```bash
export TOPIC="Remote Code Execution"
```

### 0) Setup directories

```bash
make topic-setup
```

### 1) Generate topic YAML

```bash
make topic-gen TOPIC="$TOPIC"
```

Writes:

- `configs/categories/_generated/Remote_Code_Execution.yaml`

### 2) Build / refresh the link queue

```bash
make topic-pull SOURCES=configs/sources/common.json
```

This produces a merged queue and helper flags:

- `data/Links_Queue.csv`
- `data/Links_Queue_sorted_flags.csv`

### 3) Select winners

```bash
make topic-select
```

Writes:

- `data/Selected_Remote_Code_Execution.csv`

**Fallback note:** selection prefers **exact include matches** and will **underfill** rather than pad with weak semantic matches. Semantic fallback is only used when there are *zero* strict matches.

If you want the legacy behavior (pad to `winners`), run the selector directly:

```bash
python scripts/category_select.py \
  --in data/Links_Queue_sorted_flags.csv \
  --category configs/categories/_generated/Remote_Code_Execution.yaml \
  --out data/Selected_Remote_Code_Execution.csv \
  --fill-to-winners --min-qsim 0.10
```

### 4) Scrape

```bash
make topic-scrape WINNERS=25 CONCURRENCY=4 THROTTLE_SEC=1 IGNORE_ROBOTS=1
```

Writes:

- `results/scrape_log.csv`
- `results/scraped_corpus.jsonl`
- `artifacts/` (saved HTML/PDF)

### 5) Export + verify

```bash
python scripts/export_ctikg_input.py \
  --in_jsonl results/scraped_corpus.jsonl \
  --out_csv exports/ctikg_input.csv \
  --out_docs data/ctikg_docs_meta.json

python scripts/verify_export.py
```

---

## Multi-topic / HPC runs (recommended)

The Make targets write to shared paths (`results/`, `exports/`, etc.), which collide under batch runs.

For multi-topic scaling, use `scripts/run_open_topic.py`, which writes each topic into an isolated run directory.

```bash
python scripts/run_open_topic.py \
  --topic-yaml configs/categories/_generated/Remote_Code_Execution.yaml \
  --queue data/Links_Queue_sorted_flags.csv \
  --concurrency 6 --throttle-sec 1 \
  --min-qsim 0.10
```

Outputs land in:

- `runs/<SAFE_TOPIC>/<timestamp>/...`

You can also run a whole directory of YAMLs sequentially:

```bash
python scripts/batch_run_open_topic.py \
  --topics-dir configs/categories/_generated \
  --queue data/Links_Queue_sorted_flags.csv \
  --runs-root runs \
  --concurrency 6 --throttle-sec 1
```

For SOL / HPC: use the canonical ranked-offset path documented in `docs/SOL_RUNBOOK.md` and `sol_jobs/open_topic_ranked_offsets_array.slurm`.


## Repeat-topic operator loop

For this phase, the recommended multi-topic pattern is:

1. choose a topic
2. run a bounded open-topic pass
3. inspect relevance and keep the best supported batch
4. export article-level notebook handoff files manually:
   - `python scripts/export_llm4cti_articles.py --run-dir runs/<SAFE_TOPIC>/<RUN_ID>`
5. package evidence for that run
6. move to the next viable topic

This repo does **not** require every topic to reach a fixed size target.
The better operator pattern is to capture the best relevant batch a topic supports, preserve provenance, and proceed to the next topic.

For local sequential runs across many YAMLs, `scripts/batch_run_open_topic.py` is available.
For SOL / HPC, prefer the canonical ranked-offset path in `docs/SOL_RUNBOOK.md`.


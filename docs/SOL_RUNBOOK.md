# SOL Runbook

## Purpose

This runbook documents the canonical SOL operating pattern for this repo.

This repo is a topic-focused CTI article acquisition and export front-end for CTIKG / LLM4CTI-style downstream workflows.

It does:
- generate or accept a topic
- build and rank a candidate queue
- scrape a bounded selected batch
- export verified per-run artifacts
- produce article-level handoff files for the current notebook workflow

It does not:
- replace the downstream graph-extraction notebook or package
- guarantee large viable batches for every topic
- commit generated outputs to git

## Canonical SOL pattern

Use this pattern on SOL:

1. On the login node, generate `topic.yaml` once.
2. On the login node, build the queue once.
3. On the login node, rank once and stage:
   - `config/topic.yaml`
   - `queue/Links_Queue_sorted_flags.csv`
   - `selection/ranked.csv`
4. In Slurm, run ranked-offset array tasks that only:
   - slice `ranked.csv` into `selected.csv`
   - scrape selected URLs
   - export `ctikg_input.csv`
   - verify the export
   - write `manifest.json`

Do not call Voyager / OpenAI topic generation from Slurm arrays.

## Prerequisites

- repo clone under `$HOME`, for example `~/ctikg-sol-phase1`
- Python virtual environment in `.venv`
- dependencies installed from `requirements.txt`
- access to the provider used for login-node topic generation, if you are not using `dry-run`

Important:
- the array jobs do not call Voyager / OpenAI
- the array script still expects `LLM_MODEL` to be set so the manifest records model provenance
- the canonical array script disables scrape cache in arrays to avoid shared-filesystem sqlite contention

## 1) Login-node environment setup

```bash
cd ~/ctikg-sol-phase1
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

If you are using an OpenAI-compatible endpoint such as Voyager for topic generation, export provider variables on the login node only:

```bash
export LLM_PROVIDER=openai
export OPENAI_BASE_URL="https://openai.rc.asu.edu/v1"
export OPENAI_API_KEY="YOUR_KEY"
export LLM_MODEL="llama4-scout-17b"
```

For local dry-run smoke setup instead:

```bash
export LLM_PROVIDER=dry-run
export LLM_MODEL=ignored
```

## 2) Stage one topic on the login node

Set the topic and stage paths first:

```bash
cd ~/ctikg-sol-phase1
source .venv/bin/activate

TOPIC="SSH Credential Abuse and Lateral Movement"
SAFE_TOPIC=$(python -c 'import re,sys; print(re.sub(r"[^A-Za-z0-9]+","_",sys.argv[1]).strip("_") or "Topic")' "$TOPIC")
STAGE_DIR="runs/_stage/$SAFE_TOPIC"

SCRAPE_MAX=50
STRIDE=$SCRAPE_MAX
SOURCES="configs/sources/common.json"

mkdir -p "$STAGE_DIR"/config "$STAGE_DIR"/queue "$STAGE_DIR"/selection
printf 'TOPIC=%s\nSAFE_TOPIC=%s\nSTAGE_DIR=%s\nSCRAPE_MAX=%s\n' "$TOPIC" "$SAFE_TOPIC" "$STAGE_DIR" "$SCRAPE_MAX"
```

Generate the staged topic YAML once:

```bash
python scripts/gen_category_from_llm.py \
  --topic "$TOPIC" \
  --provider "$LLM_PROVIDER" \
  --model "$LLM_MODEL" \
  --winners "$SCRAPE_MAX" \
  --out "$STAGE_DIR/config/topic.yaml"
```

Build the staged queue snapshot once:

```bash
cp "$SOURCES" "$STAGE_DIR/queue/sources.json"
cp "configs/Category_Keywords_Expanded.json" "$STAGE_DIR/queue/categories.json"

python scripts/pre_rank_links_v3.py \
  --sources "$STAGE_DIR/queue/sources.json" \
  --categories "$STAGE_DIR/queue/categories.json" \
  --out "$STAGE_DIR/queue/batch_topic.csv" \
  --limit_per_feed 600 \
  --half_life_days 999 \
  --verbose

cp "$STAGE_DIR/queue/batch_topic.csv" "$STAGE_DIR/queue/Links_Queue.csv"

python scripts/make_helper_flags.py \
  --in "$STAGE_DIR/queue/Links_Queue.csv" \
  --out "$STAGE_DIR/queue/Links_Queue_sorted_flags.csv" \
  --no-triage
```

Rank once and stage the ranked candidate list:

```bash
python scripts/category_select.py \
  --in "$STAGE_DIR/queue/Links_Queue_sorted_flags.csv" \
  --category "$STAGE_DIR/config/topic.yaml" \
  --ranked-out "$STAGE_DIR/selection/ranked.csv" \
  --selected-out "$STAGE_DIR/selection/selected_preview.csv" \
  --scrape-max "$SCRAPE_MAX" \
  --offset 0

wc -l "$STAGE_DIR/selection/ranked.csv" "$STAGE_DIR/selection/selected_preview.csv"
```

Notes:
- `selected_preview.csv` is only a login-node sanity-check slice
- the Slurm array jobs will slice from `selection/ranked.csv`
- if `ranked.csv` is very small, reduce expectations or choose a better-supported topic before launching a larger array

## 3) Submit the ranked-offset array

Count ranked rows and size the array from the staged `ranked.csv`:

```bash
RANKED_ROWS=$(python -c 'import csv,sys; print(max(sum(1 for _ in csv.reader(open(sys.argv[1], newline="", encoding="utf-8"))) - 1, 0))' "$STAGE_DIR/selection/ranked.csv")
TASKS=$(( (RANKED_ROWS + SCRAPE_MAX - 1) / SCRAPE_MAX ))

printf 'RANKED_ROWS=%s\nTASKS=%s\n' "$RANKED_ROWS" "$TASKS"
[ "$TASKS" -gt 0 ] || { echo "No ranked rows available for array submission"; exit 1; }
```

Export only the variables the array jobs actually need:

```bash
CONCURRENCY=1
THROTTLE_SEC=1
IGNORE_ROBOTS=1

export TOPIC SAFE_TOPIC STAGE_DIR SCRAPE_MAX STRIDE CONCURRENCY THROTTLE_SEC IGNORE_ROBOTS LLM_MODEL
```

Submit the canonical Slurm array script:

```bash
JOBID=$(sbatch --parsable \
  --array="0-$((TASKS - 1))%4" \
  --time=04:00:00 \
  --export=TOPIC,SAFE_TOPIC,STAGE_DIR,SCRAPE_MAX,STRIDE,CONCURRENCY,THROTTLE_SEC,IGNORE_ROBOTS,LLM_MODEL \
  sol_jobs/open_topic_ranked_offsets_array.slurm)

printf 'JOBID=%s\n' "$JOBID"
```

Notes:
- `STRIDE` should normally equal `SCRAPE_MAX`
- `%4` is an example array cap; lower or raise it based on cluster guidance and source behavior
- the canonical array script writes each shard under:
  - `runs/<SAFE_TOPIC>/slurm-<JOBID>/offset_<OFFSET>/`

## 4) Monitor and inspect outputs

Monitor while jobs are running:

```bash
squeue -j "$JOBID"
ls -lh logs | egrep "$JOBID" || true
```

Inspect final Slurm status after completion:

```bash
sacct -j "$JOBID" --format=JobID,JobName,State,ExitCode,Elapsed,MaxRSS -P
```

Inspect representative run directories:

```bash
find "runs/$SAFE_TOPIC/slurm-$JOBID" -maxdepth 2 -type d | sort
```

Inspect one representative shard:

```bash
RUN_DIR="runs/$SAFE_TOPIC/slurm-$JOBID/offset_0"

sed -n '1,120p' "$RUN_DIR/manifest.json"
head -n 5 "$RUN_DIR/scrape/scrape_log.csv"
head -n 5 "$RUN_DIR/exports/ctikg_input.csv"
```

## 5) Create article-first handoff artifacts after the run

The canonical SOL array path does not create `llm4cti/` files automatically.

Create them explicitly for any run directory you want to hand off downstream:

```bash
python scripts/export_llm4cti_articles.py --run-dir "runs/$SAFE_TOPIC/slurm-$JOBID/offset_0"
```

This writes:

- `llm4cti/Articles.xlsx`
- `llm4cti/llm4cti_articles.csv`
- `llm4cti/llm4cti_articles_meta.json`

## 6) Evidence to retain

Retain evidence outside `/tmp` and outside git:

- staged `topic.yaml`
- staged `Links_Queue_sorted_flags.csv`
- staged `ranked.csv`
- Slurm script used
- `sacct` output for the array
- representative `logs/*.out` and `logs/*.err`
- representative run directories with:
  - `manifest.json`
  - `scrape/scrape_log.csv`
  - `scrape/scrape_stats.json`
  - `scrape/scraped_corpus.jsonl`
  - `exports/ctikg_input.csv`
  - `data/ctikg_docs_meta.json`
  - `llm4cti/` exports, if produced

## 7) Operator expectations

Expect this workflow to produce:
- a bounded, auditable CTI article batch for a topic
- a verified sentence-level export
- an article-first handoff path for the current notebook workflow

Do not expect it to:
- solve topic-ranking robustness for every topic
- guarantee that every topic supports large ranked-offset batches
- replace the downstream extraction notebook or package

## 8) Repeat-topic workflow

For repeat operation, use this loop:

1. probe a topic locally or with a small staged run
2. confirm the ranked pool is large enough to justify arrays
3. capture the best relevant batch with provenance
4. export article-level handoff artifacts
5. preserve evidence
6. move to the next topic

## 9) Related docs

- `README.md`
- `docs/index.md`
- `docs/OPEN_TOPIC_QUICKSTART.md`
- `docs/OUTPUTS_CONTRACT.md`
- `docs/PIPELINE.md`
- `docs/LLM4CTI_NOTEBOOK_BRIDGE.md`
- `docs/LLM4CTI_COMPAT_TEST.md`
- `docs/TROUBLESHOOTING.md`

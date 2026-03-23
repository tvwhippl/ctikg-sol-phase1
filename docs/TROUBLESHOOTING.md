# Troubleshooting

This document prioritizes the **current per-run open-topic workflow** and the **canonical SOL staged/array workflow**.

Legacy shared-path notes are kept later in the file, but the recommended path is:

- `make open-topic ...` for local or single-run use
- the staged ranked-offset array pattern in `docs/SOL_RUNBOOK.md` for SOL

## 1) Provider setup issues

### Ollama is not responding

**Symptoms**
- local topic generation fails
- `curl` to the Ollama API fails
- `make open-topic ... PROVIDER=ollama ...` fails before queue/ranking completes

**Fix**

```bash
brew services restart ollama
ollama list || true
ollama pull llama3.1:8b
curl -s http://127.0.0.1:11434/api/tags | jq . | head
```

Confirm your environment variables:

```bash
export LLM_PROVIDER=ollama
export LLM_MODEL=llama3.1:8b
export LLM_BASE_URL="http://127.0.0.1:11434"
```

### OpenAI-compatible topic generation fails

**Symptoms**
- topic generation fails before the queue is built
- authentication or model-selection errors appear
- the provider endpoint returns 401, 403, 404, or model-not-found style errors

**Fix**

Confirm the required environment variables:

```bash
export LLM_PROVIDER=openai
export OPENAI_BASE_URL="https://openai.rc.asu.edu/v1"
export OPENAI_API_KEY="YOUR_KEY"
export LLM_MODEL="llama4-scout-17b"
```

Also verify:
- your API key was created in the Voyager User Administration portal under `LLM Access`
- `LLM_MODEL` matches a currently available Voyager model
- you did not leave the variables empty, because the helper may otherwise fall back to Ollama defaults

Optional connectivity checks:

```bash
curl -s "$OPENAI_BASE_URL/chat/completions" \
  -H "Authorization: Bearer $OPENAI_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"model":"'"$LLM_MODEL"'","messages":[{"role":"user","content":"ping"}]}' \
  | head
```

```bash
python - <<'PY'
from openai import OpenAI

client = OpenAI(
    base_url="https://openai.rc.asu.edu/v1",
    api_key="YOUR_KEY",
)

response = client.chat.completions.create(
    model="llama4-scout-17b",
    messages=[{"role": "user", "content": "ping"}],
)

print(response.choices[0].message.content)
PY
```

If you only want a smoke test without live provider calls, use:

```bash
export LLM_PROVIDER=dry-run
export LLM_MODEL=ignored
```


## 2) `make open-topic` created a run directory, but the run looks empty

Use the `run_dir` printed at the end of the command. Then inspect the selection outputs first.

```bash
RUN_DIR="runs/<SAFE_TOPIC>/<RUN_ID>"

wc -l "$RUN_DIR/selection/ranked.csv" "$RUN_DIR/selection/selected.csv"
cat "$RUN_DIR/selection/selection_summary.json"
head -n 5 "$RUN_DIR/scrape/scrape_log.csv" 2>/dev/null || true
ls -lh "$RUN_DIR/scrape/scraped_corpus.jsonl" "$RUN_DIR/exports/ctikg_input.csv" "$RUN_DIR/data/ctikg_docs_meta.json" 2>/dev/null || true
```

Interpretation:
- empty `ranked.csv` with `selection_summary.json` showing `no_candidates_passing_quality_gate` or `no_candidates_passing_anchor_gate` means the selector stopped on quality
- non-empty `ranked.csv` but empty `selected.csv` means the requested `OFFSET` is beyond the quality-gated ranked pool
- non-empty `selected.csv` but empty scrape/export outputs means scraping failed or produced no usable article text

Useful follow-up checks:

```bash
sed -n '1,200p' "$RUN_DIR/manifest.json" 2>/dev/null || true
head -n 20 "$RUN_DIR/selection/selected.csv"
cat "$RUN_DIR/selection/selection_summary.json"
head -n 20 "$RUN_DIR/scrape/scrape_log.csv" 2>/dev/null || true
```

## 3) Selection is underfilled

Underfill is not automatically a bug.

A run may still be acceptable when:
- the selected batch is smaller than `SCRAPE_MAX`
- the ranked pool is smaller than expected but still relevant

Treat underfill as a topic/ranking limitation unless logs show an actual execution fault.

What to inspect:

```bash
RUN_DIR="runs/<SAFE_TOPIC>/<RUN_ID>"

wc -l "$RUN_DIR/selection/ranked.csv" "$RUN_DIR/selection/selected.csv"
cat "$RUN_DIR/selection/selection_summary.json"
head -n 20 "$RUN_DIR/selection/ranked.csv"
```

What to do next:
- read `stop_reason` in `selection_summary.json`
- if `qsim_rejected_by_anchor_count` is large, the topic or anchors are admitting too much broad content before the gate
- reduce expectations for that topic
- tighten `fallback_anchors` or raise `fallback_anchor_min_hits` if single-anchor matches are still too broad
- prefer a smaller relevant batch over padding low-quality links

## 4) SOL array job issues

For the canonical SOL path, use the staged ranked-offset array workflow in `docs/SOL_RUNBOOK.md`.

### A shard failed in Slurm

Do not guess from one missing output file alone.

Inspect Slurm status first:

```bash
JOBID="<jobid>"

sacct -j "$JOBID" --format=JobID,JobName,State,ExitCode,Elapsed,MaxRSS -P
ls -lh logs | egrep "$JOBID" || true
```

Then inspect the specific shard logs:

```bash
sed -n '1,220p' "logs/ot_ranked_${JOBID}_0.out" 2>/dev/null || true
sed -n '1,220p' "logs/ot_ranked_${JOBID}_0.err" 2>/dev/null || true
```

If needed, inspect the shard run directory:

```bash
RUN_DIR="runs/<SAFE_TOPIC>/slurm-$JOBID/offset_<OFFSET>"

find "$RUN_DIR" -maxdepth 2 -type f | sort
sed -n '1,160p' "$RUN_DIR/manifest.json" 2>/dev/null || true
head -n 20 "$RUN_DIR/scrape/scrape_log.csv" 2>/dev/null || true
```

Also inspect the login-node stage summary for the topic:

```bash
cat "runs/_stage/<SAFE_TOPIC>/selection/selection_summary.json" 2>/dev/null || true
```

Interpretation:
- failed Slurm state + missing run outputs usually means execution failed before completion
- completed Slurm state + missing `llm4cti/` outputs usually just means the manual article-export step was not run yet
- empty shard output with an offset beyond the ranked pool can be a clean no-op rather than a real failure
- `selection_summary.json` belongs to the login-node stage, not the shard

### Arrays should not call LLM generation

The canonical SOL array script is only for:
- slice
- scrape
- export
- verify
- manifest

If an operator tries to push topic generation into arrays, stop and move that step back to the login node.

### `LLM_MODEL` is unset in arrays

The canonical array jobs do not call Voyager / OpenAI, but the script still records `LLM_MODEL` in `manifest.json`.

If `LLM_MODEL` is unset, export it before `sbatch`:

```bash
export LLM_MODEL="llama4-scout-17b"
```

### Ranked pool is smaller than the intended array

Check the staged ranked rows before submission:

```bash
wc -l "runs/_stage/<SAFE_TOPIC>/selection/ranked.csv"
```

If the ranked pool is too small:
- reduce the number of tasks
- lower expectations for that topic
- avoid treating a small ranked pool as a scraping-system bug

## 5) Shared filesystem vs `/tmp`

Prefer retained evidence and working paths under shared storage in `$HOME`, not `/tmp`.

Why:
- `/tmp` may be node-local
- contents may not persist the way you expect after job completion
- retained evidence is harder to collect consistently from `/tmp`

Keep:
- staged inputs
- Slurm logs
- representative run directories
- evidence bundles

under shared filesystem paths.

## 6) Cache behavior

For local or single-run open-topic use, cache can help avoid repeated re-scraping.

For canonical SOL arrays, cache should remain off to avoid shared-filesystem sqlite contention.

If you see cache-related confusion:
- local open-topic path may use `.cache/ctikg/scrape_cache.sqlite`
- canonical SOL array script disables cache intentionally

Do not treat “cache disabled” in arrays as a misconfiguration.

## 7) `llm4cti/` outputs are missing

That alone does not mean the run failed.

The current repo contract is:
- scrape/export/verify happen in the main run
- article-first notebook handoff is a manual post-run step

Create the notebook handoff files explicitly:

```bash
python scripts/export_llm4cti_articles.py --run-dir "runs/<SAFE_TOPIC>/<RUN_ID>"
```

Or for a SOL shard:

```bash
python scripts/export_llm4cti_articles.py --run-dir "runs/<SAFE_TOPIC>/slurm-<JOBID>/offset_<OFFSET>"
```

## 8) Legacy shared-path workflow issues

Older commands that write to:
- `data/`
- `results/`
- `exports/`

can collide across repeated runs.

If outputs appear to overwrite each other or look mixed across runs, switch back to the per-run open-topic path:

```bash
make open-topic TOPIC="..." PROVIDER=... MODEL=... SCRAPE_MAX=...
```

## 9) Quick triage checklist

When something looks wrong, check in this order:

1. provider env vars
2. `selection/ranked.csv`
3. `selection/selected.csv`
4. `scrape/scrape_log.csv`
5. `exports/ctikg_input.csv`
6. `manifest.json`
7. Slurm `sacct` and logs, if on SOL

## Related docs

- `docs/index.md`
- `docs/OPEN_TOPIC_QUICKSTART.md`
- `docs/SOL_RUNBOOK.md`
- `docs/OUTPUTS_CONTRACT.md`
- `docs/PIPELINE.md`

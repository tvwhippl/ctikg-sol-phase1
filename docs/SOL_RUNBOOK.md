# SOL Runbook

## Recommended SOL pattern

Use this pattern on SOL:

1. On the login node, generate `topic.yaml` once.
2. On the login node, build the queue once.
3. On the login node, rank once and stage:
   - `config/topic.yaml`
   - `queue/Links_Queue_sorted_flags.csv`
   - `selection/ranked.csv`
4. In Slurm, run ranked-offset array tasks that:
   - slice `ranked.csv` into `selected.csv`
   - scrape selected URLs
   - export `ctikg_input.csv`
   - verify export
   - write `manifest.json`

## Important rule

Do **not** place Voyager / OpenAI topic-generation calls inside Slurm arrays.

LLM generation should happen once on the login node.

## Canonical Slurm script

Use:

- `sol_jobs/open_topic_ranked_offsets_array.slurm`

## Required staged inputs

Expected stage layout:

- `runs/_stage/<SAFE_TOPIC>/config/topic.yaml`
- `runs/_stage/<SAFE_TOPIC>/queue/Links_Queue_sorted_flags.csv`
- `runs/_stage/<SAFE_TOPIC>/selection/ranked.csv`

## Example submission pattern

Set environment variables as needed, then submit with `sbatch`, for example:

- topic name
- stage directory
- scrape max
- stride
- concurrency
- throttle
- array size / cap

## Monitoring

Use Slurm-native monitoring:

- `sacct`
- Slurm log files in `logs/`

## Evidence to retain

Retain evidence outside `/tmp`:

- representative run directories
- `manifest.json`
- `scrape_log.csv`
- `scrape_stats.json`
- `scraped_corpus.jsonl`
- `ctikg_input.csv`
- article-level bridge artifacts
- `sacct` output
- Slurm logs

## Downstream handoff

After a run completes, export article-level notebook handoff artifacts with:

- `scripts/export_llm4cti_articles.py`

This produces:

- `llm4cti/Articles.xlsx`
- `llm4cti/llm4cti_articles.csv`
- `llm4cti/llm4cti_articles_meta.json`

# Documentation index

This repo is a **topic-focused CTI article acquisition and export front-end**
for CTIKG / LLM4CTI-style downstream workflows.

It is built to:
- define or generate a topic
- build and rank a candidate link queue
- scrape a bounded selected batch
- export verified per-run artifacts
- produce article-level handoff files for the current notebook workflow

It is **not**:
- the final official downstream packaged Python pipeline
- the full CTIKG / LLM4CTI platform
- a repo for meeting, PI, or presentation materials

## Start here

1. `README.md`
   - install, environment variables, and top-level usage

2. `docs/OPEN_TOPIC_QUICKSTART.md`
   - recommended per-run open-topic workflow

3. `docs/SOL_RUNBOOK.md`
   - canonical SOL operating pattern

4. `docs/OUTPUTS_CONTRACT.md`
   - what outputs mean, which are primary, and what counts as a successful run

5. `docs/PIPELINE.md`
   - stage-by-stage behavior and artifact layout

6. `docs/LLM4CTI_NOTEBOOK_BRIDGE.md`
   - primary downstream handoff for the current notebook workflow

7. `docs/LLM4CTI_COMPAT_TEST.md`
   - downstream compatibility proof
   - not the official final packaged downstream pipeline

8. `docs/TROUBLESHOOTING.md`
   - common failure modes and recovery steps

## Recommended workflow

### Local or single-run

Run:

`make open-topic TOPIC="..." PROVIDER=... MODEL=... SCRAPE_MAX=...`

Then create notebook handoff artifacts explicitly:

`python scripts/export_llm4cti_articles.py --run-dir runs/<SAFE_TOPIC>/<RUN_ID>`

### SOL

Use the staged pattern in `docs/SOL_RUNBOOK.md`:
- generate `topic.yaml` once on the login node
- build and stage queue / ranked inputs once
- run ranked-offset arrays only for:
  - slice
  - scrape
  - export
  - verify
  - manifest
- keep Voyager / OpenAI topic-generation calls out of arrays

## Output expectations

Immediate per-run outputs:
- `runs/<SAFE_TOPIC>/<RUN_ID>/scrape/scraped_corpus.jsonl`
- `runs/<SAFE_TOPIC>/<RUN_ID>/exports/ctikg_input.csv`
- `runs/<SAFE_TOPIC>/<RUN_ID>/data/ctikg_docs_meta.json`
- `runs/<SAFE_TOPIC>/<RUN_ID>/scrape/scrape_log.csv`
- `runs/<SAFE_TOPIC>/<RUN_ID>/manifest.json`

Manual post-run notebook handoff:
- `runs/<SAFE_TOPIC>/<RUN_ID>/llm4cti/Articles.xlsx`
- `runs/<SAFE_TOPIC>/<RUN_ID>/llm4cti/llm4cti_articles.csv`
- `runs/<SAFE_TOPIC>/<RUN_ID>/llm4cti/llm4cti_articles_meta.json`

## Legacy docs

`docs/QUICKSTART.md` documents the older shared-path workflow under
`data/`, `results/`, and `exports/`.

Prefer the open-topic per-run workflow unless you specifically need the legacy path.

# Troubleshooting

## Ollama

**Symptom:** `Error: ollama server not responding – could not find ollama app`  
**Fix:**

```bash
brew services restart ollama
ollama list || true
ollama pull llama3.1:8b
curl -s http://127.0.0.1:11434/api/tags | jq . | head
```

## Scrape produced 0 rows / export is empty

- Inspect the log:
  ```bash
  awk -F, 'NR>1{{c[$2]++}} END{{for(k in c) printf "%-12s %d\n", k, c[k]}}' results/scrape_log.csv
  head -n 5 results/scrape_log.csv
  ```
- Common mitigations:
  - Lower `CONCURRENCY` (e.g., 3–4).
  - Set `THROTTLE_SEC=1..2`.
  - If you own the sources, run with `IGNORE_ROBOTS=1`.
  - Ensure `data/Selected_*.csv` actually has rows (rerun `make topic-select WINNERS=25`).

## `BrokenPipeError` when using `-h | sed`

- Harmless: the pager closed the pipe early after printing help. Run the Python command without piping to `sed`/`head` to view full usage.

## `NameError` / missing columns in `make_helper_flags.py`

- The script now **normalizes column names** (supports `url/URL`, `title/Title`, `source_domain/Source_Domain`, etc.).
- If you pass a custom CSV, provide it explicitly:
  ```bash
  python scripts/make_helper_flags.py data/Links_Queue.csv
  ```

## Verify step fails

- Run the export again and then verify:
  ```bash
  python scripts/export_ctikg_input.py --in_jsonl results/scraped_corpus.jsonl     --out_csv exports/ctikg_input.csv --out_docs data/ctikg_docs_meta.json
  python scripts/verify_export.py
  ```

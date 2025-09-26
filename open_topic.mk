
topic-scrape:
\t@export CAT=$$(ls -t configs/categories/_generated/*.yaml | head -n1) && \\
\texport CATNAME=$$(python3 - <<'PY'\nimport yaml,sys,re\np=sys.argv[1]\nprint(re.sub(r'[^A-Za-z0-9]+','_',yaml.safe_load(open(p))['name']).strip('_'))\nPY $$CAT) && \\
\tpython3 scripts/scrape_selected.py \\
\t  --in data/Selected_$${CATNAME}.csv \\
\t  --out results/scrape_$${CATNAME}.csv \\
\t  --jsonl results/scraped_corpus.jsonl \\
\t  --artifacts artifacts \\
\t  --max_per_category $(WINNERS) \\
\t  --throttle_sec $(RATE)

topic-chunk:
\tpython3 scripts/export_ctikg_input.py \\
\t  --in_jsonl results/scraped_corpus.jsonl \\
\t  --out_csv exports/ctikg_input.csv \\
\t  --out_docs data/ctikg_docs_meta.json

# export is now a no-op alias (kept for readability)
topic-export:
\t@true

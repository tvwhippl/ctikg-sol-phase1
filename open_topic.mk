# ---- Open-topic overrides (uses hard tabs) ----
.PHONY: topic-gen topic-pull topic-select topic-scrape topic-chunk topic-export

topic-gen:
	@[ -n "$$TOPIC" ] || { echo "Set TOPIC='…' (e.g. TOPIC='ci/cd runner poisoning via OIDC & self-hosted actions')"; exit 2; }
	python3 scripts/gen_category_from_llm.py --topic "$$TOPIC" --provider "$${LLM_PROVIDER:-ollama}" --model "$${LLM_MODEL:-llama3.1}"

topic-pull:
	python3 scripts/pre_rank_links_v3.py --sources "$(SOURCES)" --categories configs/Category_Keywords_Expanded.json --out batch_topic.csv --limit_per_feed 600 --half_life_days 9999 --verbose
	python3 scripts/merge_dedup.py data/Links_Queue_master.csv data/Links_Queue.csv batch_topic.csv
	python3 scripts/make_helper_flags.py data/Links_Queue.csv

# Pick most-recent YAML and compute a safe CATNAME without heredocs
topic-select:
	CAT=$$(ls -t configs/categories/_generated/*.yaml | head -n1) ; \
	CATNAME=$$(python3 - <<'PY'\nimport yaml,sys,re\np=sys.argv[1]\nprint(re.sub(r'[^A-Za-z0-9]+','_',yaml.safe_load(open(p))['name']).strip('_'))\nPY $$CAT) ; \
	python3 scripts/category_select.py --in data/Links_Queue_sorted_flags.csv --category $$CAT ; \
	python3 scripts/scrape_selected.py --winners data/Selected_$${CATNAME}.csv --outdir content/text --rate $(RATE) --timeout $(TIMEOUT)

topic-scrape:
	CAT=$$(ls -t configs/categories/_generated/*.yaml | head -n1) ; \
	CATNAME=$$(python3 - <<'PY'\nimport yaml,sys,re\np=sys.argv[1]\nprint(re.sub(r'[^A-Za-z0-9]+','_',yaml.safe_load(open(p))['name']).strip('_'))\nPY $$CAT) ; \
	python3 scripts/scrape_selected.py --winners data/Selected_$${CATNAME}.csv --outdir content/text --rate $(RATE) --timeout $(TIMEOUT)
topic-chunk:
	@CAT=$$(ls -t configs/categories/_generated/*.yaml | head -n1) && \
	CATNAME=$$(python3 -c 'import yaml,sys,re;print(re.sub(r"[^A-Za-z0-9]+","_",yaml.safe_load(open(sys.argv[1]))["name"]).strip("_"))' $$CAT) && \
	python3 scripts/chunk_articles.py --category $$CATNAME --indir content/text --outdir chunks || true && \
	python3 scripts/export_ctikg_input.py \
	  --in_jsonl results/scraped_corpus.jsonl \
	  --out_csv $$(OUT_CSV) \
	  --out_docs $$(OUT_DOCS)
# export is a no-op alias (kept for readability)
topic-export: topic-chunk

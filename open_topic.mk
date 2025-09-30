# ---- Open-topic overrides (uses hard tabs) ----
SOURCES ?= configs/sources/common.json
RATE ?= 1.0
TIMEOUT ?= 12
WINNERS ?= 150
CONCURRENCY ?= 4
THROTTLE_SEC ?= 0

.PHONY: topic-setup topic-gen topic-pull topic-select topic-scrape topic-chunk topic-export

topic-setup:
	mkdir -p results artifacts exports configs/categories/_generated content/text chunks
	: > results/.gitkeep; : > artifacts/.gitkeep; : > exports/.gitkeep; : > configs/categories/_generated/.gitkeep || true

topic-gen:
	@[ -n "$(TOPIC)" ] || ( echo "Set TOPIC= (e.g. TOPIC='ci/cd runner poisoning via OIDC & self-hosted actions')"; exit 2 )
	python3 scripts/gen_category_from_llm.py --topic "$(TOPIC)" --provider "$${LLM_PROVIDER:-ollama}" --model "$${LLM_MODEL:-llama3.1}"

topic-pull:
	python3 scripts/pre_rank_links_v3.py --sources "$(SOURCES)" --categories configs/Category_Keywords_Expanded.json --out batch_topic.csv --limit_per_feed 600 --half_life_days 9999 --verbose
	python3 scripts/merge_dedupe.py data/Links_Queue_master.csv data/Links_Queue.csv batch_topic.csv
	python3 scripts/make_helper_flags.py data/Links_Queue.csv

# Pick most-recent YAML and compute a safe CATNAME
topic-select:
	CAT=$$(ls -t configs/categories/_generated/*.yaml | head -n1) && \
	CATNAME=$$(python3 - <<'PY' $$CAT \
import yaml,sys,re; p=sys.argv[1]; print(re.sub(r'[^A-Za-z0-9]+','_',yaml.safe_load(open(p))['name']).strip('_')) \
PY) && \
	python3 scripts/category_select.py --in data/Links_Queue_sorted_flags.csv --category $$CAT && \
	python3 scripts/scrape_selected.py \
		--in_path data/Selected_$${CATNAME}.csv \
		--out LOG_CSV results/scrape_log.csv \
		--jsonl results/scraped_corpus.jsonl \
		--artifacts artifacts \
		--max_per_category $${WINNERS} \
		--concurrency $${CONCURRENCY} \
		--ignore_robots \
		--throttle_sec $${THROTTLE_SEC}

topic-scrape: topic-select
	@CAT=$$(ls -t configs/categories/_generated/*.yaml | head -n1) && \
	CATNAME=$$(python3 - <<'PY' "$$CAT"
import yaml,sys,re; print(re.sub(r'[^A-Za-z0-9]+','_',yaml.safe_load(open(sys.argv[1]))['name']).strip('_'))
PY
) && \
	if python3 scripts/scrape_selected.py -h 2>&1 | grep -q -- '--in_path'; then \
		echo "[scrape] using NEW CLI"; \
		python3 scripts/scrape_selected.py \
			--in_path data/Selected_$${CATNAME}.csv \
			--out_log_csv results/scrape_log.csv \
			--jsonl results/scraped_corpus.jsonl \
			--artifacts artifacts \
			--max_per_category $$(WINNERS) \
			--concurrency $$(CONCURRENCY) \
			--ignore_robots \
			--throttle_sec $$(THROTTLE_SEC); \
	else \
		echo "[scrape] using OLD CLI"; \
		python3 scripts/scrape_selected.py \
			--in data/Selected_$${CATNAME}.csv \
			--out results/scrape_log.csv \
			--jsonl results/scraped_corpus.jsonl \
			--artifacts artifacts \
			--max_per_category $$(WINNERS) \
			--concurrency $$(CONCURRENCY) \
			--ignore_robots \
			--throttle_sec $$(THROTTLE_SEC); \
	fi

topic-chunk:
	CAT=$$(ls -t configs/categories/_generated/*.yaml | head -n1) && \
	CATNAME=$$(python3 - <<'PY' $$CAT \
import yaml,sys,re; p=sys.argv[1]; print(re.sub(r'[^A-Za-z0-9]+','_',yaml.safe_load(open(p))['name']).strip('_')) \
PY) && \
	python3 scripts/chunk_articles.py --category $$CATNAME --indir content/text --outdir chunks || true && \
	python3 scripts/export_ctikg_input.py \
		--in_jsonl results/scraped_corpus.jsonl \
		--out_csv exports/ctikg_input.csv \
		--out_docs data/ctikg_docs_meta.json

# export is an alias for readability
topic-export: topic-chunk

# ========== Open-topic pipeline (Ollama) ==========
# Requires GNU make (or Apple make) with TAB-prefixed recipes.

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

# Defaults (override on the CLI like: make topic-pull SOURCES=configs/sources/common.json)
SOURCES     ?= configs/sources/common.json
RATE        ?= 12
TIMEOUT     ?= 7
WINNERS     ?= 150
CONCURRENCY ?= 4
THROTTLE_SEC?= 0
IGNORE_ROBOTS?= 1
PY         ?= python3

# LLM defaults
LLM_PROVIDER ?= ollama
LLM_MODEL    ?= llama3.1

.PHONY: topic-setup topic-gen topic-pull topic-select topic-scrape topic-chunk topic-export topic-all

# Create run dirs & .gitkeep placeholders
topic-setup:
	@mkdir -p results artifacts exports configs/categories/_generated content/text chunks data
	@> results/.gitkeep
	@> artifacts/.gitkeep
	@> exports/.gitkeep
	@> configs/categories/_generated/.gitkeep

# 1) Generate a category YAML from an LLM topic prompt
#    Usage: make topic-gen TOPIC="ci/cd runner poisoning via OIDC & self-hosted actions"
topic-gen:
	@[ -n "$(TOPIC)" ] || { echo "Set TOPIC=.. (e.g. TOPIC='ci/cd runner poisoning via OIDC & self-hosted actions')"; exit 2; }
	@$(PY) scripts/gen_category_from_llm.py --topic "$(TOPIC)" --provider "$(LLM_PROVIDER)" --model "$(LLM_MODEL)"

# 2) Pull links and build the queue
topic-pull:
	@$(PY) scripts/pre_rank_links_v3.py --sources "$(SOURCES)" --categories configs/Category_Keywords_Expanded.json --out batch_topic.csv --limit_per_feed 600 --half_life_days 9999 --verbose
	@$(PY) scripts/merge_dedup.py data/Links_Queue_master.csv data/Links_Queue.csv batch_topic.csv
	@$(PY) scripts/make_helper_flags.py data/Links_Queue.csv

# Helper macro: emit CAT (path) and CATNAME (safe name) for latest generated YAML
define _cat_eval
CAT=$$(ls -t configs/categories/_generated/*.yaml | head -n1) ;\
[ -n "$$CAT" ] || { echo "No generated category YAML found in configs/categories/_generated/"; exit 2; } ;\
CATNAME=$$($(PY) -c "import yaml,sys,re; p=sys.argv[1]; print(re.sub(r'[^A-Za-z0-9]+','_',yaml.safe_load(open(p))['name']).strip('_'))" $$CAT)
endef

# 3) Select winners for the latest category
topic-select:
	@{ \
		$(call _cat_eval); \
		echo "Selecting with $$CAT"; \
		$(PY) scripts/category_select.py --in data/Links_Queue_sorted_flags.csv --category $$CAT; \
		CATCSV="data/Selected_$${CATNAME}.csv"; \
		if [ ! -s "$$CATCSV" ]; then echo "[WARN] No winners wrote for '$$CAT' → $$CATCSV"; exit 2; fi; \
		wc -l "$$CATCSV"; \
	}

# 4) Scrape winners → results/scraped_corpus.jsonl (+ artifacts, log)
#    CLI: --in (selected.csv), --out (log csv), --jsonl (corpus)
topic-scrape:
	@{ \
		$(call _cat_eval); \
		echo "Scraping $$CATNAME (max $(WINNERS))"; \
		$(PY) scripts/scrape_selected.py \
			--in "data/Selected_$$CATNAME.csv" \
			--out results/scrape_log.csv \
			--jsonl results/scraped_corpus.jsonl \
			--artifacts artifacts \
			--max_per_category $(WINNERS) \
			--concurrency $(CONCURRENCY) $(if $(filter 1,$(IGNORE_ROBOTS)),--ignore_robots) \
			--throttle_sec $(THROTTLE_SEC); \
		ls -lh results/scraped_corpus.jsonl results/scrape_log.csv; \
	}

# 5) (Optional) Write plaintext chunks, then export CTI-KG inputs and doc metadata
topic-chunk:
	@{ \
		$(call _cat_eval); \
		echo "Chunking plaintext for $$CATNAME (ok if content/text is empty; step is optional)"; \
		set +e; $(PY) scripts/chunk_articles.py --category "$$CATNAME" --indir content/text --outdir chunks; status=$$?; set -e; \
		echo "[info] chunk_articles.py exit=$$status (ignored if nonzero)"; \
		$(PY) scripts/export_ctikg_input.py \
			--in_jsonl results/scraped_corpus.jsonl \
			--out_csv exports/ctikg_input.csv \
			--out_docs data/ctikg_docs_meta.json; \
		ls -lh exports/ctikg_input.csv data/ctikg_docs_meta.json; \
	}

# Alias kept for readability
topic-export: topic-chunk

# Convenience: full run (requires TOPIC=...)
topic-all: topic-setup topic-gen topic-pull topic-select topic-scrape topic-chunk

.PHONY: topic-verify topic-export-only

topic-verify:
	@$(PY) scripts/verify_export.py

topic-export-only:
	@$(PY) scripts/export_ctikg_input.py \
		--in_jsonl results/scraped_corpus.jsonl \
		--out_csv exports/ctikg_input.csv \
		--out_docs data/ctikg_docs_meta.json




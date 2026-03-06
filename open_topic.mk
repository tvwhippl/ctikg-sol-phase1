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

# v1 open-topic (per-run isolated)
RUNS_ROOT   ?= runs
SCRAPE_MAX  ?= 50
OFFSET     ?= 0

# Scrape cache (safe-by-default for v1 open-topic: enabled)
SCRAPE_CACHE ?= 1
SCRAPE_CACHE_DB ?= .cache/ctikg/scrape_cache.sqlite
SCRAPE_CACHE_TTL_DAYS ?= 30

# Bounded rescue fill (default: off)
RESCUE ?= 0
RESCUE_MAX_ADD ?= 25
RESCUE_MIN_QSIM ?= 0.10


# Canonical one-command vars (aliases for backwards-compatible LLM_PROVIDER/LLM_MODEL)
PROVIDER    ?= $(LLM_PROVIDER)
MODEL       ?= $(LLM_MODEL)

.PHONY: topic-setup topic-gen topic-pull topic-select topic-scrape topic-chunk topic-export topic-all open-topic verify

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
	@$(MAKE) topic-setup
	@$(PY) scripts/pre_rank_links_v3.py \
		--sources $(SOURCES) \
		--categories configs/Category_Keywords_Expanded.json \
		--out batch_topic.csv \
		--limit_per_feed 600 \
		--half_life_days 999 \
		--verbose

	@wc -l batch_topic.csv | awk '{ if ($$1 <= 1) { print "ERROR: batch_topic.csv empty"; exit 1 } }'

	# Merge batch into persistent queue WITHOUT overwriting batch
	@$(PY) scripts/merge_dedup.py \
    	data/Links_Queue_master.csv \
    	data/Links_Queue.csv \
    	batch_topic.csv \
    	--no-clobber-batch

	@$(PY) scripts/make_helper_flags.py data/Links_Queue.csv

# Helper macro: set CAT (path) + CATNAME (safe name).
#
# By default we use the most recently generated YAML under configs/categories/_generated.
# For reproducible / multi-topic runs, prefer passing an explicit CAT=path/to/topic.yaml.
define _cat_eval
if [ -n "$(CAT)" ]; then \
  CAT="$(CAT)"; \
else \
  CAT=$$(ls -t configs/categories/_generated/*.yaml 2>/dev/null | head -n1); \
fi ;\
[ -n "$$CAT" ] || { echo "No category YAML resolved (set CAT=... or run make topic-gen)."; exit 2; } ;\
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




# ========== v1 Open Topic (one-command) ==========
# Usage:
#   make open-topic TOPIC="..." PROVIDER=openai MODEL=... SCRAPE_MAX=50
#
# Writes per-run outputs under:
#   runs/<SAFE_TOPIC>/<RUN_ID>/{config,queue,selection,scrape,artifacts,exports,data}

open-topic:
	@[ -n "$(TOPIC)" ] || { echo "Set TOPIC=.. (e.g. TOPIC=\"NFS File Share Exposure\")"; exit 2; }
	@{ \
		TOPIC_STR="$(TOPIC)"; \
		SAFE_TOPIC=$$($(PY) -c "import re,sys; print(re.sub(r\"[^A-Za-z0-9]+\",\"_\",sys.argv[1]).strip(\"_\") or \"Topic\")" "$$TOPIC_STR"); \
		RUN_ID=$$($(PY) -c "import datetime,os; print(datetime.datetime.now(datetime.timezone.utc).strftime(\"%Y%m%d-%H%M%S\") + \"-\" + str(os.getpid()))"); \
		RUN_DIR="$(RUNS_ROOT)/$$SAFE_TOPIC/$$RUN_ID"; \
		RUN_STARTED_AT=$$(date -u +"%Y-%m-%dT%H:%M:%SZ"); \
		DUR_GEN_YAML=0; DUR_QUEUE=0; DUR_SELECT=0; DUR_SCRAPE=0; DUR_EXPORT=0; DUR_VERIFY=0; \
		VERIFY_STATUS=unknown; VERIFY_RC=0; \
		echo "[open-topic] topic=$$TOPIC_STR"; \
		echo "[open-topic] provider=$(PROVIDER) model=$(MODEL)"; \
		echo "[open-topic] run_dir=$$RUN_DIR"; \
		mkdir -p "$$RUN_DIR"/{config,queue,selection,scrape,artifacts,exports,data}; \
		\
		t_step=$$(date +%s); \
		# 1) Generate a per-run topic YAML (no reliance on latest generated YAML) \
		$(PY) scripts/gen_category_from_llm.py \
			--topic "$$TOPIC_STR" \
			--provider "$(PROVIDER)" \
			--model "$(MODEL)" \
			--winners $(SCRAPE_MAX) \
			--out "$$RUN_DIR/config/topic.yaml"; \
		DUR_GEN_YAML=$$(( $$(date +%s) - t_step )); \
		\
		t_step=$$(date +%s); \
		# 2) Build a per-run queue snapshot \
		cp "$(SOURCES)" "$$RUN_DIR/queue/sources.json"; \
		cp "configs/Category_Keywords_Expanded.json" "$$RUN_DIR/queue/categories.json"; \
		$(PY) scripts/pre_rank_links_v3.py \
			--sources "$$RUN_DIR/queue/sources.json" \
			--categories "$$RUN_DIR/queue/categories.json" \
			--out "$$RUN_DIR/queue/batch_topic.csv" \
			--limit_per_feed 600 \
			--half_life_days 999 \
			--verbose; \
		wc -l "$$RUN_DIR/queue/batch_topic.csv" | awk '{ if ($$1 <= 1) { print "ERROR: batch_topic.csv empty"; exit 1 } }'; \
		cp "$$RUN_DIR/queue/batch_topic.csv" "$$RUN_DIR/queue/Links_Queue.csv"; \
		$(PY) scripts/make_helper_flags.py \
			--in "$$RUN_DIR/queue/Links_Queue.csv" \
			--out "$$RUN_DIR/queue/Links_Queue_sorted_flags.csv" \
			--no-triage; \
		DUR_QUEUE=$$(( $$(date +%s) - t_step )); \
		\
		t_step=$$(date +%s); \
		# 3) Select + scrape + export + verify \
		$(PY) scripts/category_select.py \
			--in "$$RUN_DIR/queue/Links_Queue_sorted_flags.csv" \
			--category "$$RUN_DIR/config/topic.yaml" \
			--ranked-out "$$RUN_DIR/selection/ranked.csv" \
			--selected-out "$$RUN_DIR/selection/selected.csv" \
			--scrape-max $(SCRAPE_MAX) \
			--offset $(OFFSET) \
			$(if $(filter 1,$(RESCUE)),--rescue-underfill --rescue-max-add $(RESCUE_MAX_ADD) --rescue-min-qsim $(RESCUE_MIN_QSIM)); \
		wc -l "$$RUN_DIR/selection/selected.csv" | awk '{ if ($$1 <= 1) { print "ERROR: selection empty"; exit 2 } }'; \
		DUR_SELECT=$$(( $$(date +%s) - t_step )); \
		\
		t_step=$$(date +%s); \
		$(PY) scripts/scrape_selected.py \
			--in "$$RUN_DIR/selection/selected.csv" \
			--out "$$RUN_DIR/scrape/scrape_log.csv" \
			--jsonl "$$RUN_DIR/scrape/scraped_corpus.jsonl" \
			--artifacts "$$RUN_DIR/artifacts" \
			--max_per_category $(SCRAPE_MAX) \
			--concurrency $(CONCURRENCY) $(if $(filter 1,$(IGNORE_ROBOTS)),--ignore_robots) \
			--throttle_sec $(THROTTLE_SEC) \
			$(if $(filter 1,$(SCRAPE_CACHE)),--cache-db "$(SCRAPE_CACHE_DB)" --cache-ttl-days $(SCRAPE_CACHE_TTL_DAYS),--no-cache) \
			--stats-json "$$RUN_DIR/scrape/scrape_stats.json"; \
		DUR_SCRAPE=$$(( $$(date +%s) - t_step )); \
		\
		t_step=$$(date +%s); \
		$(PY) scripts/export_ctikg_input.py \
			--in_jsonl "$$RUN_DIR/scrape/scraped_corpus.jsonl" \
			--out_csv "$$RUN_DIR/exports/ctikg_input.csv" \
			--out_docs "$$RUN_DIR/data/ctikg_docs_meta.json" \
			--log_csv "$$RUN_DIR/scrape/scrape_log.csv"; \
		DUR_EXPORT=$$(( $$(date +%s) - t_step )); \
		\
		t_step=$$(date +%s); \
		set +e; \
		$(PY) scripts/verify_export.py \
			--corpus "$$RUN_DIR/scrape/scraped_corpus.jsonl" \
			--csv "$$RUN_DIR/exports/ctikg_input.csv"; \
		VERIFY_RC=$$?; \
		set -e; \
		DUR_VERIFY=$$(( $$(date +%s) - t_step )); \
		if [ $$VERIFY_RC -eq 0 ]; then VERIFY_STATUS=pass; else VERIFY_STATUS=fail; fi; \
		RUN_FINISHED_AT=$$(date -u +"%Y-%m-%dT%H:%M:%SZ"); \
		\
		$(PY) scripts/write_run_manifest.py \
			--run-dir "$$RUN_DIR" \
			--topic "$$TOPIC_STR" \
			--provider "$(PROVIDER)" \
			--model "$(MODEL)" \
			--scrape-max $(SCRAPE_MAX) \
			--offset $(OFFSET) \
			--concurrency $(CONCURRENCY) \
			--throttle-sec $(THROTTLE_SEC) \
			--ignore-robots $(IGNORE_ROBOTS) \
			--cache-enabled $(SCRAPE_CACHE) \
			--cache-db "$(SCRAPE_CACHE_DB)" \
			--cache-ttl-days $(SCRAPE_CACHE_TTL_DAYS) \
			--rescue-enabled $(RESCUE) \
			--rescue-max-add $(RESCUE_MAX_ADD) \
			--rescue-min-qsim $(RESCUE_MIN_QSIM) \
			--run-started-at-utc "$$RUN_STARTED_AT" \
			--run-finished-at-utc "$$RUN_FINISHED_AT" \
			--dur-gen-yaml-sec $$DUR_GEN_YAML \
			--dur-queue-sec $$DUR_QUEUE \
			--dur-select-sec $$DUR_SELECT \
			--dur-scrape-sec $$DUR_SCRAPE \
			--dur-export-sec $$DUR_EXPORT \
			--dur-verify-sec $$DUR_VERIFY \
			--verify-status $$VERIFY_STATUS; \
		\
		if [ $$VERIFY_RC -ne 0 ]; then exit $$VERIFY_RC; fi; \
		echo "[OK] run_dir=$$RUN_DIR"; \
	}

# Verify wrapper:
# - If RUN_DIR is set, verify that run's outputs.
# - Otherwise, verify legacy default paths.
verify:
	@{ \
		if [ -n "$(RUN_DIR)" ]; then \
			$(PY) scripts/verify_export.py --corpus "$(RUN_DIR)/scrape/scraped_corpus.jsonl" --csv "$(RUN_DIR)/exports/ctikg_input.csv"; \
		else \
			$(PY) scripts/verify_export.py; \
		fi; \
	}

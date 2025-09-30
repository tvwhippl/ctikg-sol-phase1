# ==== Open-topic overrides (uses hard tabs) =====
.PHONY: topic-setup topic-gen topic-pull topic-select topic-scrape topic-chunk topic-export topic

# ---- default knobs (can be overridden) ----
LLM_PROVIDER ?= ollama
LLM_MODEL    ?= llama3.1

SOURCES      ?= configs/sources/common.json
WINNERS      ?= 150
RATE         ?= 1.0
TIMEOUT      ?= 12
CONCURRENCY  ?= 4
THROTTLE     ?= 1

OUT_CSV      ?= exports/ctikg_input.csv
OUT_DOCS     ?= data/ctikg_docs_meta.json

# ---- make run dirs once per clone ----
topic-setup:
	@mkdir -p results artifacts exports data chunks content/text

# ---- 1) generate a category YAML via Ollama ----
topic-gen:
	@[ -n "$$TOPIC" ] || (echo "Set TOPIC='...'" ; exit 2)
	python3 scripts/gen_category_from_llm.py --topic "$$TOPIC" --provider "$${LLM_PROVIDER}" --model "$${LLM_MODEL}"

# ---- 2) pull feeds + score ----
topic-pull:
	python3 scripts/pre_rank_links_v3.py --sources "$(SOURCES)" --categories configs/Category_Keywords_Expanded.json --out batch_topic.csv --limit_per_feed 600 --half_life_days 9999 --verbose
	python3 scripts/merge_dedup.py data/Links_Queue_master.csv data/Links_Queue.csv batch_topic.csv
	python3 scripts/make_helper_flags.py data/Links_Queue.csv

# Helper: pick latest YAML + derive a safe CATNAME
define _pickcat
CAT=$$(ls -t configs/categories/_generated/*.yaml | head -n1) ;\
CATNAME=$$(python3 - <<'PY' "$$CAT" \
import yaml,sys,re; p=sys.argv[1];\
print(re.sub(r"[^A-Za-z0-9]+","_",yaml.safe_load(open(p))["name"]).strip("_"))
PY \
) ;\
echo $$CATNAME
endef

# ---- 3) select winners for this category ----
topic-select:
	$(eval CAT    := $(shell ls -t configs/categories/_generated/*.yaml | head -n1))
	$(eval CATNAME:= $(shell python3 - <<'PY' "$(CAT)" \
import yaml,sys,re; p=sys.argv[1];\
print(re.sub(r"[^A-Za-z0-9]+","_",yaml.safe_load(open(p))["name"]).strip("_"))
PY))
	@echo "Selecting with SCAT: $(CATNAME)"
	python3 scripts/category_select.py --in data/Links_Queue_sorted_flags.csv --category "$(CAT)"
	python3 scripts/scrape_selected.py \
		--in "data/Selected_$(CATNAME).csv" \
		--jsonl results/scraped_corpus.jsonl \
		--log_csv results/scrape_log.csv \
		--artifacts artifacts \
		--max_per_category $(WINNERS) \
		--concurrency $(CONCURRENCY) \
		--throttle_sec $(THROTTLE)

# ---- 4) (optional) just scraping step (if you want it separate) ----
topic-scrape: topic-select
	@true

# ---- 5) chunk articles and export CTIKG inputs ----
topic-chunk:
	$(eval CAT    := $(shell ls -t configs/categories/_generated/*.yaml | head -n1))
	$(eval CATNAME:= $(shell python3 - <<'PY' "$(CAT)" \
import yaml,sys,re; p=sys.argv[1];\
print(re.sub(r"[^A-Za-z0-9]+","_",yaml.safe_load(open(p))["name"]).strip("_"))
PY))
	python3 scripts/chunk_articles.py --category "$(CATNAME)" --indir content/text --outdir chunks || true
	python3 scripts/export_ctikg_input.py \
		--in_jsonl results/scraped_corpus.jsonl \
		--out_csv $(OUT_CSV) \
		--out_docs $(OUT_DOCS)

# make "export" a readable alias (no-op; export is done in topic-chunk)
topic-export: ; @true

# ---- 6) Drive the whole pipeline after topic-gen ----
topic: topic-setup topic-pull topic-select topic-scrape topic-chunk topic-export
	@echo "All done. csv=$(OUT_CSV) docs=$(OUT_DOCS)"

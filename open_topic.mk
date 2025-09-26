topic-select:
	@set -eu; \
	CAT="$$(ls -t configs/categories/_generated/*.yaml | head -n1)"; \
	CATNAME="$$(python3 -c "import yaml,sys,re; p=sys.argv[1]; print(re.sub(r'[^A-Za-z0-9]+','_', yaml.safe_load(open(p))['name']).strip('_'))" "$$CAT")"; \
	echo "Selecting with $$CAT ($$CATNAME)"; \
	python3 scripts/category_select.py --in data/Links_Queue_sorted_flags.csv --category "$$CAT" --winners "data/Selected_$$CATNAME.csv"

topic-scrape:
	@set -eu; \
	CAT="$$(ls -t configs/categories/_generated/*.yaml | head -n1)"; \
	CATNAME="$$(python3 -c "import yaml,sys,re; p=sys.argv[1]; print(re.sub(r'[^A-Za-z0-9]+','_', yaml.safe_load(open(p))['name']).strip('_'))" "$$CAT")"; \
	echo "Scraping articles for $$CATNAME"; \
	python3 scripts/scrape_selected.py \
		--in "data/Selected_$$CATNAME.csv" \
		--jsonl results/scraped_corpus.jsonl \
		--artifacts artifacts \
		--max_per_category $(WINNERS) \
		--concurrency 4

topic-chunk:
	@set -eu; \
	test -s results/scraped_corpus.jsonl || { echo "missing results/scraped_corpus.jsonl (run make topic-scrape)"; exit 1; }; \
	mkdir -p exports; \
	python3 scripts/export_ctikg_input.py \
		--in_jsonl results/scraped_corpus.jsonl \
		--out_csv exports/ctikg_input.csv \
		--out_docs data/ctikg_docs_meta.json

# export is now a no-op alias (kept for readability)
topic-export:
\t@true

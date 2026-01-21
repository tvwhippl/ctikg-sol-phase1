export LLM_PROVIDER=ollama
export LLM_MODEL=llama3.1:8b-instruct
#!/usr/bin/env bash
set -euo pipefail

# Run Phase-1 smoke (assumes tests/smoke_phase1.sh already exists)
if [ -f tests/smoke_phase1.sh ]; then
  echo "Running Phase-1 smoke test..."
  ./tests/smoke_phase1.sh
else
  echo "Phase-1 smoke test not found; please run the Phase-1 pipeline manually first" >&2
  exit 1
fi

# Ensure results exist
if [ ! -f results/scraped_corpus.jsonl ]; then
  echo "ERROR: results/scraped_corpus.jsonl not found" >&2
  exit 2
fi

OUTDIR=outputs/simple_ctikg
mkdir -p "$OUTDIR"

# Run adapter (OpenRouter by default)
python scripts/run_simple_ctikg.py \
  --input results/scraped_corpus.jsonl \
  --output "$OUTDIR" \
  --max-docs 5 \
  --min-chars 200 \
  --llm-provider openrouter \
  --llm-model google/gemma-2-9b-it

# verify outputs
if [ ! -s "$OUTDIR/simple_ctikg_graph.json" ]; then
  echo "ERROR: simple_ctikg_graph.json missing or empty" >&2
  exit 3
fi
if [ ! -s "$OUTDIR/simple_ctikg_graph.html" ]; then
  echo "ERROR: simple_ctikg_graph.html missing or empty" >&2
  exit 4
fi

echo "SMOKE TEST PASSED"

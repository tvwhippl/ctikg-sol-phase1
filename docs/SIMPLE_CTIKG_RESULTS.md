# Simple CTIKG run (summary)

- Date: $(date)
- Input docs: exports/ctikg_input.csv (1709 rows)
- Simple CTIKG outputs: results/simple_ctikg_results.jsonl (1709 lines)
- Docs with usable triples: 27
- Docs with no triples: 1682
- Notes:
  - Simple CTIKG is single-shot extraction (one LLM call per doc) — conservative extractor.
  - Many CTI feed items are short / low-signal; expect low recall for naive extraction.
  - Visualization produced via scripts/visualize_ctikg_fallback.py (top 150 nodes).
  - Next steps: move to full CTIKG multi-pass extraction for higher recall and entity resolution.


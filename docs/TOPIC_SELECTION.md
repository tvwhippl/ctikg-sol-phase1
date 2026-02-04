# Topic Selection (category_select)

Purpose: produce candidate categories for CTI ingestion (label, canonical_id, description, examples, confidence).

Files:
 - scripts/category_select.py (CLI + API: select_topics())
 - scripts/gen_category_from_llm.py (LLM wrapper)

Usage examples (local):
  python3 scripts/category_select.py --mode deterministic --query "remote code execution" --seed docs/article1.txt docs/article2.txt --k 12 --out /tmp/categories.json

LLM usage (SOL / Docker):
 - Set one of:
   - OPENAI_BASE_URL and OPENAI_API_KEY (OpenAI-compatible endpoint)
   - OLLAMA_HOST (e.g. http://localhost:11434)
   - OPENROUTER_API_KEY and OPENROUTER_ENDPOINT
 - Optional: OPENAI_MODEL, OLLAMA_MODEL, OPENROUTER_ENDPOINT override.

Environment variables:
 - OPENAI_BASE_URL, OPENAI_API_KEY
 - OLLAMA_HOST
 - OPENROUTER_API_KEY, OPENROUTER_ENDPOINT
 - OPENAI_MODEL (optional)

Failure modes & controls:
 - Use `--dry-run` to simulate LLM calls (returns a canned response).
 - `--max-llm-tokens` and `--max-llm-queries` available to bound cost.
 - `--fallback-to-deterministic` will revert to deterministic behavior on LLM failures.

Notes:
 - Deterministic mode uses TF-IDF + KMeans; it's deterministic (fixed random_state).
 - Hybrid runs deterministic then asks LLM to *refine/merge* candidates — LLM is not allowed to invent unconstrained categories.
 - LLM prompts require JSON-only replies; results are validated and sanitized before use.

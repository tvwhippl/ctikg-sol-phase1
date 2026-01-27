#!/usr/bin/env python3
"""
Simple CTIKG runner with provider support for Ollama (default) and OpenRouter.

Usage examples:
  # Ollama (default)
  export OLLAMA_HOST="http://127.0.0.1:11434"
  python scripts/run_simple_ctikg.py --input exports/ctikg_input_small.csv --out results/simple_ctikg_results_small.jsonl

  # OpenRouter smoke test (10 docs)
  export OPENROUTER_API_KEY="sk_..."
  python scripts/run_simple_ctikg.py --provider openrouter --input exports/ctikg_input_small.csv --out results/or_simple_ctikg_small.jsonl --max-docs 10

Safety:
  - Default throttle (200 ms) to avoid rate limits.
  - --max-docs default is unlimited, but anything >25 requires --confirm explicitly.
  - Use --dry-run to preview requests without sending.
"""
import os
import sys
import time
import csv
import json
import argparse
from tqdm import tqdm
import requests

def sanitize_generated_text(generated_text):
    """
    Normalize LLM output so JSON can be parsed reliably.
    Removes markdown fences, trims wrappers, and extracts JSON blocks.
    """
    if not isinstance(generated_text, str):
        return generated_text

    s = generated_text.strip()

    # Remove fenced code blocks ``` or ```json
    if s.startswith("```"):
        parts = s.split("```")
        if len(parts) >= 3:
            s = parts[1].strip()

    # Remove leading language hints
    if s.lower().startswith("json"):
        s = s[4:].strip()

    # Strip wrapping quotes
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()

    # If text contains extra prose, extract the JSON object
    if not s.startswith("{") and "{" in s and "}" in s:
        start = s.find("{")
        end = s.rfind("}")
        s = s[start:end + 1]

    return s

try:
    from json_repair import repair
except Exception:
    repair = None

# ----------------------
# Low-level helpers
# ----------------------
def get_available_ollama_models(ollama_host):
    try:
        r = requests.get(ollama_host.rstrip('/') + "/v1/models", timeout=10)
        r.raise_for_status()
        data = r.json().get("data", [])
        return [m["id"] for m in data]
    except Exception:
        return []

def parse_llm_response(resp_json):
    """
    Normalize possible response shapes from Ollama / OpenAI-like endpoints.
    Returns the generated string (as best detected).
    """
    if resp_json is None:
        return None

    # Ollama style: {'choices': [{'message': {'content': '...'}}]}
    if isinstance(resp_json, dict):
        choices = resp_json.get("choices")
        if isinstance(choices, list) and len(choices) > 0:
            first = choices[0]
            # nested message
            if isinstance(first.get("message"), dict) and first["message"].get("content"):
                return first["message"]["content"]
            # older style may have 'text' or 'message' as text
            if first.get("text"):
                return first.get("text")
            if first.get("message") and isinstance(first.get("message"), str):
                return first.get("message")

        # direct textual fields
        for k in ("text", "completion", "output", "result"):
            if k in resp_json and isinstance(resp_json[k], str):
                return resp_json[k]

    # fallback: try to stringify entire response
    try:
        return json.dumps(resp_json, ensure_ascii=False)
    except Exception:
        return str(resp_json)

# ----------------------
# Provider implementations
# ----------------------
def send_to_ollama(ollama_host, model, prompt, timeout=120):
    """
    Uses Ollama v1/chat/completions endpoint.
    """
    # Validate model availability and fallback
    models = get_available_ollama_models(ollama_host)
    if models and model not in models:
        print(f"[WARN] Requested model '{model}' not available on Ollama. Falling back to '{models[0]}'")
        model = models[0]

    url = ollama_host.rstrip('/') + "/v1/chat/completions"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a cybersecurity knowledge extraction engine."},
            {"role": "user", "content": prompt}
        ]
    }
    r = requests.post(url, json=payload, timeout=timeout)
    r.raise_for_status()
    return r.json()

def send_to_openrouter(openrouter_key, model, prompt, timeout=120):
    """
    Calls the OpenRouter hosted API (OpenAI-compatible chat completions).
    IMPORTANT: ensure OPENROUTER_API_KEY is set. This method returns the JSON response.
    """
    if not openrouter_key:
        raise ValueError("OPENROUTER_API_KEY not set or empty")

    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {"Authorization": f"Bearer {openrouter_key}", "Content-Type": "application/json"}
    payload = {"model": model, "messages": [{"role": "user", "content": prompt}]}
    r = requests.post(url, headers=headers, json=payload, timeout=timeout)
    r.raise_for_status()
    return r.json()

# ----------------------
# Prompt builder
# ----------------------
def build_prompt(doc_text, doc_id=None):
    # Conservative prompt that asks for JSON and minimal hallucination.
    hdr = "Extract a JSON object with fields: doc_id (string), triples (list of {subject, relation, object, sentence (optional)})."
    hdr += " If nothing extractable, return {\"doc_id\":<id>, \"triples\": []}.\n\n"
    if doc_id is not None:
        hdr += f"Document ID: {doc_id}\n\n"
    hdr += "Text:\n" + doc_text + "\n\nOutput only valid JSON."
    return hdr

# ----------------------
# Main runner
# ----------------------
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="exports/ctikg_input.csv")
    p.add_argument("--out", default="results/simple_ctikg_results.jsonl")
    p.add_argument("--model", default=os.environ.get("CTIKG_MODEL", "llama3.2:latest"))
    p.add_argument("--ollama-host", default=os.environ.get("OLLAMA_HOST","http://127.0.0.1:11434"))
    p.add_argument("--provider", choices=["ollama","openrouter"], default="ollama")
    p.add_argument("--openrouter-key", default=os.environ.get("OPENROUTER_API_KEY"))
    p.add_argument("--col", default="text", help="CSV column with document text; if not found, read whole line")
    p.add_argument("--throttle-ms", type=int, default=200, help="ms to sleep between LLM calls")
    p.add_argument("--max-docs", type=int, default=0, help="max docs to process (0 = no limit)")
    p.add_argument("--dry-run", action="store_true", help="do not send requests; just print first prompts")
    p.add_argument("--confirm", action="store_true", help="required to run if max-docs > 25")
    args = p.parse_args()

    # Safe OpenRouter fallback: prevent forwarding local model names
    if args.provider == "openrouter":
        if args.model.startswith("llama") or ":" in args.model:
            args.model = os.environ.get("CTIKG_OR_TEST_MODEL", "gpt-4o-mini")

    if args.max_docs > 25 and not args.confirm:
        print("[SAFE] Attempting to run more than 25 docs requires --confirm. Exiting.")
        sys.exit(2)

    # Create out dir
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

    # Read input - CSV or line-per-doc
    rows = []
    if not os.path.exists(args.input):
        print("Input file not found:", args.input)
        sys.exit(2)

    with open(args.input, 'r', encoding='utf-8', errors='replace') as fh:
        first = fh.readline()
        if ',' in first and args.col in first:
            fh.seek(0)
            rdr = csv.DictReader(fh)
            for r in rdr:
                rows.append(r)
        else:
            fh.seek(0)
            for i,line in enumerate(fh):
                rows.append({args.col: line.strip(), "id": str(i)})

    if args.max_docs and args.max_docs < len(rows):
        rows = rows[:args.max_docs]

    print(f"Found {len(rows)} docs in {args.input}")

    if args.dry_run:
        print("[DRY RUN] Showing first 3 prompts:")
        for i,r in enumerate(rows[:3]):
            doc_text = r.get(args.col) or r.get("text") or ""
            print("--- PROMPT %d ---\n%s\n" % (i, build_prompt(doc_text, doc_id=r.get("id"))))
        return

    out_f = open(args.out, "w", encoding='utf-8')
    openrouter_key = args.openrouter_key or os.environ.get("OPENROUTER_API_KEY")

    for i, r in enumerate(tqdm(rows, desc="Processing")):
        doc_id = r.get("id") or r.get("doc_id") or str(i)
        doc_text = r.get(args.col) or r.get("text") or r.get("content") or ""
        prompt = build_prompt(doc_text, doc_id=doc_id)

        try:
            if args.provider == "openrouter":
                resp_json = send_to_openrouter(openrouter_key, args.model, prompt)
            else:
                resp_json = send_to_ollama(args.ollama_host, args.model, prompt)
            generated = parse_llm_response(resp_json)
        except Exception as e:
            # Log the error but continue
            generated = json.dumps({"error": str(e)})
            print(f"[ERR] doc {doc_id}: {e}")

        # parse / repair (with sanitizer)
        parsed = None
        clean_text = sanitize_generated_text(generated)

        try:
            parsed = json.loads(clean_text)
        except Exception:
            if repair:
                try:
                    repaired = repair(clean_text)
                    parsed = json.loads(repaired)
                except Exception:
                    parsed = {"_raw": generated}
            else:
                parsed = {"_raw": generated}

        usage = None
        if isinstance(resp_json, dict):
            usage = resp_json.get("usage")

        result_obj = {"doc_id": doc_id, "result": parsed}
        if usage:
            result_obj["_usage"] = usage

        out_f.write(json.dumps(result_obj, ensure_ascii=False) + "\n")

        # throttle to avoid rate-limit / cost spikes
        if args.throttle_ms:
            time.sleep(args.throttle_ms / 1000.0)

    out_f.close()
    print("Wrote results to", args.out)

if __name__ == "__main__":
    main()

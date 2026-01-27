#!/usr/bin/env python3
"""
Simple CTIKG runner that uses Ollama HTTP completions.
Input: exports/ctikg_input.csv (CSV with a 'text' column or raw doc per line)
Output: results/simple_ctikg_results.jsonl
Usage:
  export OLLAMA_HOST="http://127.0.0.1:11434"
  python scripts/run_simple_ctikg.py --input exports/ctikg_input.csv --out results/simple_ctikg_results.jsonl --model llama3.2 --viz results/simple_ctikg_viz.html
"""
import os, csv, json, time, argparse
from tqdm import tqdm
import requests
try:
    from json_repair import repair
except Exception:
    repair = None

def get_available_models(ollama_host):
    url = ollama_host.rstrip("/") + "/v1/models"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json().get("data", [])
    return [m["id"] for m in data]

def send_to_ollama(ollama_host, model, prompt, timeout=120):
    models = get_available_models(ollama_host)

    if model not in models:
        print(f"[WARN] Requested model '{model}' not found. Available models: {models}")
        model = models[0]
        print(f"[WARN] Falling back to model '{model}'")

    url = ollama_host.rstrip("/") + "/v1/chat/completions"

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a cybersecurity knowledge extraction engine."},
            {"role": "user", "content": prompt}
        ]
    }

    resp = requests.post(url, json=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()

def build_prompt(doc_text):
    # Minimal single-shot prompt (edit as you like)
    return (
        "Extract a JSON list of triples from the following CTI article text. "
        "Each triple should be an object with keys: subject, relation, object, sentence. "
        "Return a single JSON object: {\"doc_id\": <id>, \"triples\": [ ... ]}.\n\n"
        "Text:\n" + doc_text + "\n\nOutput only valid JSON."
    )

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="exports/ctikg_input.csv")
    p.add_argument("--out", default="results/simple_ctikg_results.jsonl")
    p.add_argument("--model", default="llama3.2")
    p.add_argument("--ollama-host", default=os.environ.get("OLLAMA_HOST","http://127.0.0.1:11434"))
    p.add_argument("--viz", default=None)
    p.add_argument("--col", default="text", help="CSV column with document text; if not found, read whole line")
    args = p.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    rows = []

    # Detect input format
    if not os.path.exists(args.input):
        print("Input file not found:", args.input)
        raise SystemExit(2)

    # try reading as CSV with header
    with open(args.input, 'r', encoding='utf-8', errors='replace') as fh:
        first = fh.readline()
        if ',' in first and args.col in first:
            fh.seek(0)
            rdr = csv.DictReader(fh)
            for r in rdr:
                rows.append(r)
        else:
            # fallback: treat each line as a doc
            fh.seek(0)
            for i, line in enumerate(fh):
                rows.append({args.col: line.strip(), "id": str(i)})

    print(f"Found {len(rows)} docs in {args.input}")
    out_f = open(args.out, "w", encoding='utf-8')

    for i, r in enumerate(tqdm(rows, desc="Processing")):
        doc_id = r.get("id") or r.get("doc_id") or str(i)
        doc_text = r.get(args.col) or r.get("text") or r.get("content") or ""
        prompt = build_prompt(doc_text)
        try:
            resp = send_to_ollama(args.ollama_host, args.model, prompt)
            # Ollama returns structure with 'choices' or direct 'completion' depending on version; handle generically:
            body = resp
            # Try to extract generated text:
            generated = None
            if isinstance(body, dict):
                # new Ollama style: 'choices' -> [{'message': {'content': '...'}}]
                c = body.get("choices")
                if c and isinstance(c, list):
                    generated = c[0]["message"]["content"]
                else:
                    # sometimes the model returns a 'text' or 'completion' field:
                    generated = body.get("text") or body.get("completion")
            if generated is None:
                generated = json.dumps(body)
        except Exception as e:
            print("LLM call failed for doc", doc_id, ":", e)
            generated = json.dumps({"error": str(e)})

        # attempt to repair if it is not valid JSON
        parsed = None
        try:
            parsed = json.loads(generated)
        except Exception:
            if repair:
                try:
                    repaired = repair(generated)
                    parsed = json.loads(repaired)
                except Exception:
                    parsed = {"_raw": generated}
            else:
                parsed = {"_raw": generated}

        result_obj = {"doc_id": doc_id, "result": parsed}
        out_f.write(json.dumps(result_obj, ensure_ascii=False) + "\n")
        out_f.flush()
        time.sleep(0.1)  # small throttle; adjust if needed

    out_f.close()
    print("Wrote results to", args.out)

if __name__ == "__main__":
    main()

# scripts/run_simple_ctikg.py
# Adapter: Phase-1 results -> Simple CTIKG single-prompt run
import argparse
import json
import os
import sys
from pathlib import Path

# Optional: import your LLM client wrappers here
# We'll support OpenRouter (HTTP) and Ollama (local) via simple adapters
import requests


def load_jsonl(path):
    docs = []
    with open(path, 'r', encoding='utf-8') as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                docs.append(json.loads(line))
            except Exception as e:
                print(f"WARN: failed to parse jsonl line: {e}", file=sys.stderr)
    return docs


def write_json(obj, path):
    with open(path, 'w', encoding='utf-8') as fh:
        json.dump(obj, fh, indent=2, ensure_ascii=False)


def prepare_input(docs, max_docs, min_chars):
    selected = []
    for d in docs:
        content = d.get('content') or d.get('text') or d.get('body') or ''
        title = d.get('title') or d.get('headline') or ''
        url = d.get('url') or d.get('source') or ''
        if not content or len(content) < min_chars:
            continue
        selected.append({'title': title, 'source': url, 'text': content})
        if len(selected) >= max_docs:
            break
    return selected


def build_prompt(input_docs):
    # This is intentionally simple and explicit. Keep prompt small for token safety.
    prompt = (
        "Extract entities and relations from the following CTI articles.\n"
        "Return a JSON object with two keys: \"nodes\" and \"edges\".\n"
        "Nodes should be objects with {id, label, type}. Edges should be {source, target, label}.\n"
        "Be concise. Use stable IDs (slugify label).\n\n"
    )
    for i, doc in enumerate(input_docs, 1):
        prompt += f"Article {i}: Title: {doc.get('title','(no title)')}\n"
        txt = doc.get('text','').strip()
        if len(txt) > 2000:
            txt = txt[:2000] + '...'
        prompt += f"{txt}\n\n"
    prompt += "\nReturn only valid JSON."
    return prompt


# Minimal OpenRouter client wrapper (single completion)

def openrouter_complete(api_key, base_url, model, prompt, max_tokens=1024):
    headers = {'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'}
    payload = {
        'model': model,
        'messages': [
            {'role': 'user', 'content': prompt}
        ],
        'max_tokens': max_tokens,
        'temperature': 0.0
    }
    r = requests.post(base_url, headers=headers, json=payload, timeout=120)
    r.raise_for_status()
    j = r.json()
    # openrouter chat completions structure: choices[0].message.content
    try:
        return j['choices'][0]['message']['content']
    except Exception:
        # Fallback: try raw text
        return json.dumps(j)


# Minimal Ollama wrapper (local)
def ollama_complete(base_url, model, prompt, max_tokens=1024):
    url = f"{base_url.rstrip('/')}/chat/completions"
    payload = {
        'model': model,
        'messages': [{'role': 'user', 'content': prompt}],
        'max_tokens': max_tokens,
        'temperature': 0.0
    }
    r = requests.post(url, json=payload, timeout=120)
    r.raise_for_status()
    j = r.json()
    try:
        return j['choices'][0]['message']['content']
    except Exception:
        return json.dumps(j)


def parse_json_output(raw_text):
    # Try to extract JSON substring robustly
    text = raw_text.strip()
    # naive attempt: find first { and last }
    start = text.find('{')
    end = text.rfind('}')
    if start == -1 or end == -1 or end <= start:
        raise ValueError('No JSON object found in model output')
    sub = text[start:end+1]
    try:
        return json.loads(sub)
    except Exception as e:
        # last resort: try json_repair if available
        try:
            import jsonrepair
            repaired = jsonrepair.repair(sub)
            return json.loads(repaired)
        except Exception:
            raise


def render_pyvis(graph_obj, out_html):
    try:
        from pyvis.network import Network
    except Exception:
        raise RuntimeError('pyvis not installed; pip install pyvis')
    net = Network(height='800px', width='100%', notebook=False)
    id_map = {}
    for n in graph_obj.get('nodes', []):
        nid = n.get('id') or n.get('label')
        label = n.get('label')
        t = n.get('type')
        net.add_node(nid, label=label, title=t)
        id_map[label] = nid
    for e in graph_obj.get('edges', []):
        s = e.get('source')
        t = e.get('target')
        label = e.get('label')
        net.add_edge(s, t, title=label)
    net.show(out_html)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--input', required=True, help='results/scraped_corpus.jsonl')
    ap.add_argument('--output', required=True, help='output directory')
    ap.add_argument('--max-docs', type=int, default=10)
    ap.add_argument('--min-chars', type=int, default=500)
    ap.add_argument('--category', default=None)
    ap.add_argument('--llm-provider', choices=['openrouter', 'ollama'], default='ollama')
    ap.add_argument('--llm-model', default='google/gemma-2-9b-it')
    ap.add_argument('--openrouter-api-key', default=os.environ.get('OPENROUTER_API_KEY'))
    ap.add_argument('--openrouter-base-url', default=os.environ.get('OPENROUTER_BASE_URL','https://openrouter.ai/api/v1/chat/completions'))
    ap.add_argument('--ollama-base-url', default=os.environ.get('LLM_BASE_URL','http://127.0.0.1:11434'))
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    docs = load_jsonl(args.input)
    print(f'Loaded {len(docs)} docs from {args.input}', file=sys.stderr)

    # filter by category if requested
    if args.category:
        docs = [d for d in docs if d.get('category') == args.category]
        print(f'After category filter: {len(docs)} docs', file=sys.stderr)

    selected = prepare_input(docs, args.max_docs, args.min_chars)
    print(f'Selected {len(selected)} docs for Simple CTIKG', file=sys.stderr)

    if len(selected) == 0:
        print('ERROR: no documents selected', file=sys.stderr)
        sys.exit(2)

    input_docs_path = out_dir / 'input_docs.json'
    write_json(selected, input_docs_path)

    if args.dry_run:
        print('Dry run complete', file=sys.stderr)
        sys.exit(0)

    prompt = build_prompt(selected)

    if args.llm_provider == 'openrouter':
        if not args.openrouter_api_key:
            print('ERROR: OPENROUTER_API_KEY required for OpenRouter provider', file=sys.stderr)
            sys.exit(3)
        raw = openrouter_complete(args.openrouter_api_key, args.openrouter_base_url, args.llm_model, prompt)
    else:
        raw = ollama_complete(args.ollama_base_url, args.llm_model, prompt)

    # try parse
    try:
        graph_obj = parse_json_output(raw)
    except Exception as e:
        print('ERROR: failed to parse model output as JSON:', e, file=sys.stderr)
        print('Raw model output (begin):', file=sys.stderr)
        print(raw[:4000], file=sys.stderr)
        sys.exit(4)

    out_json = out_dir / 'simple_ctikg_graph.json'
    write_json(graph_obj, out_json)

    out_html = out_dir / 'simple_ctikg_graph.html'
    try:
        render_pyvis(graph_obj, str(out_html))
    except Exception as e:
        print('WARNING: rendering failed:', e, file=sys.stderr)

    print('Simple CTIKG run complete. Outputs:', out_json, out_html, file=sys.stderr)


if __name__ == '__main__':
    main()

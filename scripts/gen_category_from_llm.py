#!/usr/bin/env python3
import argparse, os, sys, re, json, time
from typing import Dict, Any, List
import requests, yaml

SCHEMA = {
  "name": str,
  "include": list,
  "exclude": list,
  "winners": int,
}

PROMPT = """You are a security research assistant. The user will give you a TOPIC.
Produce a YAML object (no prose) with keys:
name: short human name for the category
include: 15-30 concise search terms or regex fragments that match relevant articles
exclude: 5-15 terms to filter tutorials, marketing, press releases, jobs, etc.
winners: integer target number of winners to select (default 120)

Rules:
- Output ONLY YAML. No code fences.
- Keep include terms specific (abuse patterns, techniques, product/platform names).
- Prefer phrases that appear in titles/snippets (e.g., 'workflow injection', 'OIDC', 'runner poisoning').
TOPIC: """  # user topic appended

def sanitize_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_")

def heuristic(topic: str) -> Dict[str, Any]:
    base = [topic, topic.replace("/", " "), topic.replace("-", " "), topic.lower()]
    ex = ["tutorial", "how to", "marketing", "press release", "job posting"]
    return {"name": topic.title(), "include": base, "exclude": ex, "winners": 120}

def call_ollama(model: str, prompt: str, base="http://localhost:11434") -> str:
    r = requests.post(f"{base}/api/generate", json={"model": model, "prompt": prompt, "stream": False}, timeout=120)
    r.raise_for_status()
    return r.json()["response"]

def call_openai_compatible(model: str, prompt: str, base: str, api_key: str) -> str:
    # OpenAI-compatible /v1/chat/completions
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    body = {"model": model, "messages":[{"role":"system","content":"You write YAML only."},
                                        {"role":"user","content": prompt}],
            "temperature": 0.2}
    r = requests.post(f"{base.rstrip('/')}/v1/chat/completions", headers=headers, json=body, timeout=120)
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]

def parse_yaml(yaml_text: str) -> Dict[str, Any]:
    data = yaml.safe_load(yaml_text)
    if not isinstance(data, dict): raise ValueError("YAML is not a mapping.")
    # minimal schema check / coercion
    out = {
        "name": str(data.get("name") or "Untitled Category"),
        "include": [str(x) for x in data.get("include", [])][:40],
        "exclude": [str(x) for x in data.get("exclude", [])][:40],
        "winners": int(data.get("winners") or 120),
    }
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", required=True)
    ap.add_argument("--provider", choices=["ollama","openai"], default=os.getenv("LLM_PROVIDER","ollama"))
    ap.add_argument("--model", default=os.getenv("LLM_MODEL","llama3.1"))
    ap.add_argument("--base", default=os.getenv("LLM_API_BASE","http://localhost:11434"))
    ap.add_argument("--api_key", default=os.getenv("LLM_API_KEY",""))
    ap.add_argument("--outdir", default="configs/categories/_generated")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    prompt = PROMPT + args.topic.strip()

    cfg = None
    try:
        if args.provider == "ollama":
            text = call_ollama(args.model, prompt, base=args.base)
        else:
            text = call_openai_compatible(args.model, prompt, base=args.base, api_key=args.api_key)
        cfg = parse_yaml(text)
    except Exception as e:
        # fallback heuristic
        cfg = heuristic(args.topic)

    # post-process: anchor common regex forms lightly
    def tweak(terms: List[str]) -> List[str]:
        cleaned = []
        for t in terms:
            t = t.strip().strip("/")
            if len(t) > 64: continue
            cleaned.append(t)
        # dedupe, preserve order
        seen = set(); out=[]
        for t in cleaned:
            if t.lower() in seen: continue
            seen.add(t.lower()); out.append(t)
        return out[:40]

    cfg["include"] = tweak(cfg.get("include", []))
    cfg["exclude"] = tweak(cfg.get("exclude", []))
    slug = sanitize_name(cfg["name"])
    path = os.path.join(args.outdir, f"{slug}.yaml")
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f, sort_keys=False, allow_unicode=True)
    print(f"[DONE] Wrote {path}")
    print(yaml.safe_dump(cfg, sort_keys=False, allow_unicode=True))

if __name__ == "__main__":
    main()

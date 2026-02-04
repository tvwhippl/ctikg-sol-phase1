"""
scripts/gen_category_from_llm.py

Lightweight LLM client wrapper for category_select.py

Behavior:
 - Respects env vars OPENAI_BASE_URL and OPENAI_API_KEY for OpenAI-compatible endpoints
 - Detects OLLAMA_HOST (ollama) or OPENROUTER_API_KEY/ENDPOINT usage
 - Supports --dry-run: returns cached canned response
 - Handles max_tokens and max_queries and returns parsed JSON
 - Ensures returned JSON validated to required schema
"""

from __future__ import annotations
import os
import json
import logging
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger("gen_category_from_llm")
logger.setLevel(logging.INFO)

# canned response for dry-run / examples
CANNED_RESPONSE = [
    {
        "label": "Remote Code Execution",
        "canonical_id": "remote-code-execution-1a2b3c4d",
        "short_description": "Attacks that enable execution of attacker code on remote hosts.",
        "example_article_excerpt": "An exploit allowed unauthenticated RCE via buffer overflow in X.",
        "confidence_score": 0.9,
        "examples": ["doc:example-1"]
    },
    {
        "label": "Credential Theft",
        "canonical_id": "credential-theft-5f6e7d8c",
        "short_description": "Attacks that harvest or steal user credentials.",
        "example_article_excerpt": "Malware harvested saved credentials and exfiltrated them.",
        "confidence_score": 0.8,
        "examples": ["doc:example-2"]
    }
]

def canned_llm_response_for_dry_run():
    # return deep copy
    import copy
    return copy.deepcopy(CANNED_RESPONSE)

# basic validation
REQUIRED_FIELDS = {"label"}

class LLMClient:
    def __init__(self, dry_run: bool = False, max_tokens: int = 1024, max_queries: int = 1):
        self.dry_run = dry_run
        self.max_tokens = max_tokens
        self.max_queries = max_queries
        # detect envs
        self.openai_base = os.getenv("OPENAI_BASE_URL")
        self.openai_key = os.getenv("OPENAI_API_KEY")
        self.ollama_host = os.getenv("OLLAMA_HOST")
        self.openrouter_key = os.getenv("OPENROUTER_API_KEY")
        self.openrouter_endpoint = os.getenv("OPENROUTER_ENDPOINT")
        logger.debug("LLMClient init: dry_run=%s openai_base=%s ollama=%s openrouter=%s", dry_run, bool(self.openai_base), bool(self.ollama_host), bool(self.openrouter_key))

    def call_llm_for_categories(self, prompt: Dict[str, Any], n: int = 12, max_tokens: Optional[int] = None, max_queries: Optional[int] = None, **kwargs) -> List[Dict[str, Any]]:
        """
        Returns a list of dicts with at least 'label'. May include canonical_id, short_description, confidence_score, examples.
        """
        if self.dry_run:
            logger.info("Dry-run enabled; returning canned response")
            return canned_llm_response_for_dry_run()

        if max_tokens is None:
            max_tokens = self.max_tokens
        if max_queries is None:
            max_queries = self.max_queries

        # enforce query cap
        if max_queries > 5:
            logger.warning("Max queries limited to 5 from provided %s", max_queries)
            max_queries = 5

        # Choose backend
        if self.ollama_host:
            return self._call_ollama(prompt, n=n, max_tokens=max_tokens, max_queries=max_queries)
        if self.openrouter_key and self.openrouter_endpoint:
            return self._call_openrouter(prompt, n=n, max_tokens=max_tokens, max_queries=max_queries)
        if self.openai_key and self.openai_base:
            return self._call_openai_compatible(prompt, n=n, max_tokens=max_tokens, max_queries=max_queries)
        # fallback: try official openai package
        try:
            import openai
            if os.getenv("OPENAI_API_KEY"):
                openai.api_key = os.getenv("OPENAI_API_KEY")
                return self._call_openai_library(prompt, n=n, max_tokens=max_tokens, max_queries=max_queries)
        except Exception:
            pass

        raise RuntimeError("No LLM backend configured. Set OPENAI_BASE_URL & OPENAI_API_KEY, or OLLAMA_HOST, or OPENROUTER_API_KEY & OPENROUTER_ENDPOINT, or set dry-run.")

    def _validate_and_parse(self, raw_text: str) -> List[Dict[str, Any]]:
        # raw_text expected to be JSON array. Try parse safely.
        try:
            parsed = json.loads(raw_text)
            if isinstance(parsed, dict) and "items" in parsed:
                candidates = parsed["items"]
            elif isinstance(parsed, list):
                candidates = parsed
            else:
                logger.warning("LLM returned unsupported JSON shape; attempting to coerce to list")
                candidates = parsed if isinstance(parsed, list) else []
        except Exception as e:
            logger.error("Failed to parse LLM output as JSON: %s", e)
            raise

        # validate required fields and sanitize shapes
        out = []
        for obj in candidates:
            if not isinstance(obj, dict):
                continue
            if not obj.get("label"):
                continue
            cleaned = {
                "label": str(obj.get("label")).strip(),
                "canonical_id": obj.get("canonical_id") or "",
                "short_description": obj.get("short_description") or obj.get("description") or "",
                "example_article_excerpt": obj.get("example_article_excerpt") or obj.get("example") or "",
                "confidence_score": float(obj.get("confidence_score") or obj.get("confidence") or 0.5),
                "examples": obj.get("examples") or ([] if not obj.get("example_article_excerpt") else [obj.get("example_article_excerpt")]),
            }
            out.append(cleaned)
        return out

    def _call_ollama(self, prompt: Dict[str, Any], n: int, max_tokens: int, max_queries: int) -> List[Dict[str, Any]]:
        import requests
        host = self.ollama_host.rstrip("/")
        model = os.getenv("OLLAMA_MODEL", "llama2")
        url = f"{host}/api/generate"
        payload = {
            "model": model,
            "prompt": json.dumps(prompt),
            "max_tokens": max_tokens,
            "top_k": 50,
        }
        logger.info("Calling Ollama at %s model=%s", host, model)
        r = requests.post(url, json=payload, timeout=60)
        r.raise_for_status()
        raw = r.text
        return self._validate_and_parse(raw)

    def _call_openrouter(self, prompt: Dict[str, Any], n: int, max_tokens: int, max_queries: int) -> List[Dict[str, Any]]:
        import requests
        url = self.openrouter_endpoint
        headers = {"Authorization": f"Bearer {self.openrouter_key}"}
        payload = {"input": json.dumps(prompt), "max_tokens": max_tokens, "n": 1}
        logger.info("Calling OpenRouter endpoint %s", url)
        r = requests.post(url, json=payload, headers=headers, timeout=60)
        r.raise_for_status()
        return self._validate_and_parse(r.text)

    def _call_openai_compatible(self, prompt: Dict[str, Any], n: int, max_tokens: int, max_queries: int) -> List[Dict[str, Any]]:
        import requests
        base = self.openai_base.rstrip("/")
        url = f"{base}/v1/chat/completions"
        headers = {"Authorization": f"Bearer {self.openai_key}"}
        messages = [{"role": "system", "content": prompt.get("instruction","")}, {"role":"user","content": prompt.get("user_query", json.dumps(prompt))}]
        data = {
            "model": os.getenv("OPENAI_MODEL","gpt-4o-mini"),
            "messages": messages,
            "max_tokens": max_tokens,
            "n": 1,
            "temperature": 0.0
        }
        logger.info("Calling OpenAI-compatible API at %s model=%s", base, data["model"])
        r = requests.post(url, json=data, headers=headers, timeout=60)
        r.raise_for_status()
        resp = r.json()
        # try to extract assistant content
        text = ""
        try:
            text = resp["choices"][0]["message"]["content"]
        except Exception:
            text = json.dumps(resp)
        return self._validate_and_parse(text)

    def _call_openai_library(self, prompt: Dict[str, Any], n: int, max_tokens: int, max_queries: int) -> List[Dict[str, Any]]:
        import openai
        model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        logger.info("Calling OpenAI library model=%s", model)
        resp = openai.ChatCompletion.create(
            model=model,
            messages=[{"role":"system","content":prompt.get("instruction","")}, {"role":"user","content": prompt.get("user_query", json.dumps(prompt))}],
            max_tokens=max_tokens,
            temperature=0.0,
            n=1
        )
        text = resp.choices[0].message.content
        return self._validate_and_parse(text)

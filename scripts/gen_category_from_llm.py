"""scripts/gen_category_from_llm.py

This module serves two purposes:

1) **Library**: provides `LLMClient` used by topic-candidate generation utilities.
   - `LLMClient.call_llm_for_categories(prompt, ...) -> List[dict]`

2) **CLI** (used by open_topic.mk): generates a **category YAML** suitable for
   open-topic selection.

    python3 scripts/gen_category_from_llm.py \
      --topic "Remote Code Execution" \
      --provider ollama \
      --model llama3 \
      --out configs/categories/_generated/category_YYYYMMDD_HHMMSS.yaml

Providers supported:
- `ollama` (local) via /api/chat
- `openai` / `openai-compatible` (custom base URL; e.g. ASU endpoint) via /chat/completions
- `openrouter` via OpenAI-compatible chat/completions
- `dry-run` (offline) returns a canned deterministic output

Key fix included:
- Avoids generating URLs like .../v1/v1/chat/completions if the base URL already
  includes /v1.
"""

from __future__ import annotations

import argparse
import copy
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger("gen_category_from_llm")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ------------------------------
# Canned responses
# ------------------------------

CANNED_RESPONSE: List[Dict[str, Any]] = [
    {
        "label": "Remote Code Execution",
        "canonical_id": "remote-code-execution-1a2b3c4d",
        "short_description": "Attacks that enable execution of attacker code on remote hosts.",
        "example_article_excerpt": "An exploit allowed unauthenticated RCE via buffer overflow in X.",
        "confidence_score": 0.9,
        "examples": ["doc:example-1"],
    },
    {
        "label": "Credential Theft",
        "canonical_id": "credential-theft-5f6e7d8c",
        "short_description": "Attacks that harvest or steal user credentials.",
        "example_article_excerpt": "Malware harvested saved credentials and exfiltrated them.",
        "confidence_score": 0.8,
        "examples": ["doc:example-2"],
    },
]


def canned_llm_response_for_dry_run() -> List[Dict[str, Any]]:
    return copy.deepcopy(CANNED_RESPONSE)


# ------------------------------
# Helpers
# ------------------------------

def _strip_code_fences(text: str) -> str:
    """Remove common markdown code fences."""
    if not text:
        return ""
    t = text.strip()
    # ```json ... ```
    if t.startswith("```"):
        # remove the first fence line
        t = re.sub(r"^```[a-zA-Z0-9_-]*\s*\n", "", t)
        # remove trailing fence
        t = re.sub(r"\n```\s*$", "", t)
    return t.strip()


def _extract_json(text: str) -> Any:
    """Best-effort extraction of a JSON object/array from an LLM response."""
    t = _strip_code_fences(text)

    # Fast path
    try:
        return json.loads(t)
    except Exception:
        pass

    # Heuristic: find first '{' ... last '}' or first '[' ... last ']'
    first_obj = t.find("{")
    last_obj = t.rfind("}")
    first_arr = t.find("[")
    last_arr = t.rfind("]")

    candidates: List[str] = []
    if 0 <= first_arr < last_arr:
        candidates.append(t[first_arr : last_arr + 1])
    if 0 <= first_obj < last_obj:
        candidates.append(t[first_obj : last_obj + 1])

    for c in candidates:
        try:
            return json.loads(c)
        except Exception:
            continue

    raise ValueError("Could not parse JSON from LLM response")


def _normalize_openai_base(base: str) -> str:
    """Normalize a base URL (strip trailing '/')."""
    return (base or "").rstrip("/")


def _openai_chat_completions_url(base: str) -> str:
    """Return the correct /chat/completions URL, avoiding double '/v1'."""
    base = _normalize_openai_base(base)
    if not base:
        raise RuntimeError("OPENAI_BASE_URL (or LLM_BASE_URL for provider=openai) is not set")

    # If base already ends with /v1, do NOT add another /v1.
    if base.endswith("/v1"):
        return f"{base}/chat/completions"
    return f"{base}/v1/chat/completions"


# ------------------------------
# LLM Client
# ------------------------------

@dataclass
class LLMClient:
    """Small client that supports Ollama + OpenAI-compatible chat."""

    provider: str = ""
    model: str = ""
    dry_run: bool = False
    max_tokens: int = 1024
    max_queries: int = 1

    # credentials / endpoints
    openai_base: str = ""
    openai_key: str = ""
    openrouter_key: str = ""
    openrouter_endpoint: str = ""
    ollama_host: str = ""

    def __post_init__(self) -> None:
        # defaults from env
        self.provider = (self.provider or os.getenv("LLM_PROVIDER") or "").strip() or ""
        self.model = (self.model or os.getenv("LLM_MODEL") or os.getenv("OPENAI_MODEL") or "").strip() or ""

        self.openai_key = self.openai_key or os.getenv("OPENAI_API_KEY") or os.getenv("OPENAI_KEY") or ""
        self.openai_base = self.openai_base or os.getenv("OPENAI_BASE_URL") or os.getenv("OPENAI_API_BASE") or ""

        self.openrouter_key = self.openrouter_key or os.getenv("OPENROUTER_API_KEY") or ""
        self.openrouter_endpoint = self.openrouter_endpoint or os.getenv("OPENROUTER_ENDPOINT") or "https://openrouter.ai/api/v1/chat/completions"

        # Ollama: prefer explicit OLLAMA_HOST; otherwise LLM_BASE_URL if provider=ollama
        self.ollama_host = self.ollama_host or os.getenv("OLLAMA_HOST") or ""

        # Provider inference if not specified
        if not self.provider:
            if self.openai_key:
                self.provider = "openai"
            elif self.openrouter_key:
                self.provider = "openrouter"
            else:
                self.provider = "ollama"

        # Apply provider-specific base fallbacks
        if self.provider in {"ollama", "ollama-chat"}:
            if not self.ollama_host:
                self.ollama_host = os.getenv("LLM_BASE_URL") or "http://127.0.0.1:11434"
        elif self.provider in {"openai", "openai-compatible"}:
            if not self.openai_base:
                # allow open_topic.mk style where only LLM_BASE_URL is set
                self.openai_base = os.getenv("LLM_BASE_URL") or ""

        # Model defaults per provider
        if not self.model:
            if self.provider in {"ollama", "ollama-chat"}:
                self.model = os.getenv("OLLAMA_MODEL") or "llama3"
            elif self.provider == "openrouter":
                self.model = os.getenv("OPENROUTER_MODEL") or "openai/gpt-4o-mini"
            else:
                self.model = "gpt-4o-mini"

        logger.debug(
            "LLMClient init provider=%s model=%s dry_run=%s openai_base=%s ollama_host=%s",
            self.provider,
            self.model,
            self.dry_run,
            bool(self.openai_base),
            self.ollama_host,
        )

    # ---------- High-level helpers ----------

    def call_llm_for_categories(
        self,
        prompt: Dict[str, Any],
        n: int = 12,
        max_tokens: Optional[int] = None,
        max_queries: Optional[int] = None,
        **_: Any,
    ) -> List[Dict[str, Any]]:
        """Return list[dict] with at least {label}.

        This is used by scripts/topic_candidate_select.py.
        """
        if self.dry_run or self.provider in {"dry-run", "dryrun", "mock"}:
            logger.info("Dry-run enabled; returning canned response")
            return canned_llm_response_for_dry_run()

        max_tokens = int(max_tokens or self.max_tokens)
        max_queries = int(max_queries or self.max_queries)
        max_queries = max(1, min(max_queries, 5))

        # We do a small retry loop because JSON formatting is the most common failure.
        last_err: Optional[Exception] = None
        for attempt in range(1, max_queries + 1):
            try:
                text = self._call_chat(prompt)
                obj = _extract_json(text)
                if not isinstance(obj, list):
                    raise ValueError("Expected JSON array for categories")
                return self._validate_categories(obj)
            except Exception as e:
                last_err = e
                logger.warning("LLM category call failed attempt %s/%s: %s", attempt, max_queries, e)
                time.sleep(0.2)

        raise RuntimeError(f"LLM call failed after {max_queries} attempts: {last_err}")

    def call_llm_for_topic_config(
        self,
        topic: str,
        winners: int = 100,
        max_tokens: Optional[int] = None,
        max_queries: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Return a dict with keys: name/include/exclude/fallback_anchors/fallback_anchor_min_hits/winners."""
        if self.dry_run or self.provider in {"dry-run", "dryrun", "mock"}:
            return _canned_topic_config(topic=topic, winners=winners)

        max_tokens = int(max_tokens or min(self.max_tokens, 800))
        max_queries = int(max_queries or self.max_queries)
        max_queries = max(1, min(max_queries, 5))

        prompt = {
            "instruction": (
                "You generate a single topic configuration for a security-news curation pipeline. "
                "Return ONLY a JSON object with keys: "
                "name (string), include (array of strings), exclude (array of strings), "
                "fallback_anchors (array of strings), fallback_anchor_min_hits (integer), winners (integer). "
                "Rules: include should be 8-15 short keyword phrases; exclude should be 5-12 phrases; "
                "fallback_anchors should be 2-6 high-precision substrings or short phrases used to gate semantic fallback admission; "
                "prefer product, protocol, service, or compound terms; avoid generic single words like credential, attack, vulnerability, or exploit unless paired; "
                "fallback_anchor_min_hits should be 1 or 2; "
                "Include both acronyms/abbreviations (e.g., RCE, SSRF, XSS) AND their expanded forms when applicable; "
                "avoid overly generic words; keep phrases <= 4 words; no prose outside JSON."
            ),
            "user_query": topic,
            "topic": topic,
            "winners": int(winners),
        }

        last_err: Optional[Exception] = None
        for attempt in range(1, max_queries + 1):
            try:
                text = self._call_chat(prompt, max_tokens=max_tokens)
                obj = _extract_json(text)
                if not isinstance(obj, dict):
                    raise ValueError("Expected JSON object for topic config")
                cfg = _validate_topic_config(obj, default_name=topic, default_winners=winners)
                # Force stable naming: downstream make targets + Selected_*.csv should key off the user's topic,
                # not whatever the LLM invents.
                cfg["name"] = (topic or "").strip() or cfg.get("name") or "Topic"
                return cfg

            except Exception as e:
                last_err = e
                logger.warning("LLM topic-config call failed attempt %s/%s: %s", attempt, max_queries, e)
                time.sleep(0.2)

        raise RuntimeError(f"LLM topic-config failed after {max_queries} attempts: {last_err}")

    # ---------- Provider calls ----------

    def _call_chat(self, prompt: Dict[str, Any], max_tokens: Optional[int] = None) -> str:
        """Return raw assistant content text."""
        provider = (self.provider or "").lower()

        if provider in {"ollama", "ollama-chat"}:
            return self._call_ollama_chat(prompt, max_tokens=max_tokens)

        if provider in {"openrouter"}:
            return self._call_openrouter_chat(prompt, max_tokens=max_tokens)

        if provider in {"openai", "openai-compatible"}:
            return self._call_openai_compatible_chat(prompt, max_tokens=max_tokens)

        raise RuntimeError(f"Unsupported provider: {self.provider}")

    def _call_ollama_chat(self, prompt: Dict[str, Any], max_tokens: Optional[int] = None) -> str:
        import requests

        host = (self.ollama_host or "http://127.0.0.1:11434").rstrip("/")
        url = f"{host}/api/chat"

        system = str(prompt.get("instruction") or "").strip()
        user = str(prompt.get("user_query") or json.dumps(prompt))

        payload: Dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "stream": False,
        }
        # Some Ollama versions accept num_predict, some accept options
        if max_tokens is not None:
            payload["options"] = {"num_predict": int(max_tokens)}

        logger.info("Calling Ollama at %s model=%s", host, self.model)
        r = requests.post(url, json=payload, timeout=90)
        r.raise_for_status()
        resp = r.json()
        try:
            return resp["message"]["content"]
        except Exception:
            return json.dumps(resp)

    def _call_openrouter_chat(self, prompt: Dict[str, Any], max_tokens: Optional[int] = None) -> str:
        import requests

        if not self.openrouter_key:
            raise RuntimeError("OPENROUTER_API_KEY not set")

        url = self.openrouter_endpoint
        headers = {
            "Authorization": f"Bearer {self.openrouter_key}",
            "Content-Type": "application/json",
        }

        system = str(prompt.get("instruction") or "").strip()
        user = str(prompt.get("user_query") or json.dumps(prompt))

        data: Dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": 0.0,
        }
        if max_tokens is not None:
            data["max_tokens"] = int(max_tokens)

        logger.info("Calling OpenRouter API model=%s", self.model)
        r = requests.post(url, json=data, headers=headers, timeout=90)
        r.raise_for_status()
        resp = r.json()
        try:
            return resp["choices"][0]["message"]["content"]
        except Exception:
            return json.dumps(resp)

    def _call_openai_compatible_chat(self, prompt: Dict[str, Any], max_tokens: Optional[int] = None) -> str:
        import requests

        if not self.openai_key:
            raise RuntimeError("OPENAI_API_KEY not set")

        # Normalize base and avoid .../v1/v1/... bugs
        url = _openai_chat_completions_url(self.openai_base)

        system = str(prompt.get("instruction") or "").strip()
        user = str(prompt.get("user_query") or json.dumps(prompt))

        data: Dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": 0.0,
        }
        if max_tokens is not None:
            data["max_tokens"] = int(max_tokens)

        headers = {
            "Authorization": f"Bearer {self.openai_key}",
            "Content-Type": "application/json",
        }

        logger.info("Calling OpenAI-compatible API at %s model=%s", _normalize_openai_base(self.openai_base), self.model)
        r = requests.post(url, json=data, headers=headers, timeout=90)
        r.raise_for_status()
        resp = r.json()
        try:
            return resp["choices"][0]["message"]["content"]
        except Exception:
            return json.dumps(resp)

    # ---------- Validation ----------

    def _validate_categories(self, obj: List[Any]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for item in obj:
            if not isinstance(item, dict):
                continue
            label = str(item.get("label") or "").strip()
            if not label:
                continue
            out.append(
                {
                    "label": label,
                    "canonical_id": str(item.get("canonical_id") or ""),
                    "short_description": str(item.get("short_description") or item.get("description") or ""),
                    "example_article_excerpt": str(item.get("example_article_excerpt") or item.get("example") or ""),
                    "confidence_score": float(item.get("confidence_score") or item.get("confidence") or 0.6),
                    "examples": item.get("examples") or [],
                }
            )
        return out


# ------------------------------
# Topic-config helpers (YAML)
# ------------------------------

def _canned_topic_config(topic: str, winners: int = 100) -> Dict[str, Any]:
    # This is intentionally conservative and deterministic.
    toks = [t.lower() for t in re.findall(r"[a-zA-Z0-9]+", topic or "") if len(t) >= 3]
    toks = toks[:10]
    include = list(dict.fromkeys(toks + ["exploit", "vulnerability", "patch", "cve", "proof of concept"]))
    exclude = ["movie", "music", "recipe", "sports", "wikipedia"]
    return {
        "name": topic.strip() or "Topic",
        "include": include[:15],
        "exclude": exclude,
        "fallback_anchors": [],
        "fallback_anchor_min_hits": 1,
        "winners": int(winners),
    }


def _validate_topic_config(obj: Dict[str, Any], default_name: str, default_winners: int) -> Dict[str, Any]:
    name = str(obj.get("name") or default_name or "Topic").strip() or "Topic"

    include = obj.get("include")
    if not isinstance(include, list):
        include = []
    include = [str(x).strip() for x in include if str(x).strip()]

    exclude = obj.get("exclude")
    if not isinstance(exclude, list):
        exclude = []
    exclude = [str(x).strip() for x in exclude if str(x).strip()]

    fallback_anchors = obj.get("fallback_anchors")
    if not isinstance(fallback_anchors, list):
        fallback_anchors = []
    fallback_anchors = [str(x).strip() for x in fallback_anchors if str(x).strip()]

    fallback_anchor_min_hits = obj.get("fallback_anchor_min_hits", 1)
    try:
        fallback_anchor_min_hits = int(fallback_anchor_min_hits)
    except Exception:
        fallback_anchor_min_hits = 1
    fallback_anchor_min_hits = max(1, min(fallback_anchor_min_hits, 5))

    winners = obj.get("winners", default_winners)
    try:
        winners = int(winners)
    except Exception:
        winners = int(default_winners)

    winners = max(1, min(winners, 5000))

    return {
        "name": name,
        "include": include,
        "exclude": exclude,
        "fallback_anchors": fallback_anchors,
        "fallback_anchor_min_hits": fallback_anchor_min_hits,
        "winners": winners,
    }


# ------------------------------
# CLI
# ------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Generate a category YAML from an LLM")
    ap.add_argument("--topic", required=True, help="Topic prompt (e.g., 'Remote Code Execution')")
    ap.add_argument("--provider", default=os.getenv("LLM_PROVIDER", "ollama"), help="ollama | openai | openrouter | dry-run")
    ap.add_argument("--model", default=os.getenv("LLM_MODEL", ""), help="Model name for the provider")
    ap.add_argument("--out", default=None, help="Output YAML path (default: configs/categories/_generated/<TOPIC>.yaml)")
    ap.add_argument("--winners", type=int, default=100, help="How many winners to select downstream")
    ap.add_argument("--max-tokens", type=int, default=800, help="Max tokens for the LLM response")
    ap.add_argument("--max-queries", type=int, default=1, help="Retries for malformed output")
    ap.add_argument("--dry-run", action="store_true", help="Force dry-run output")

    args = ap.parse_args()

    provider = (args.provider or "").strip().lower()
    dry_run = bool(args.dry_run) or provider in {"dry-run", "dryrun", "mock"}

    client = LLMClient(
        provider=provider,
        model=args.model,
        dry_run=dry_run,
        max_tokens=int(args.max_tokens),
        max_queries=int(args.max_queries),
    )

    cfg = client.call_llm_for_topic_config(topic=args.topic, winners=args.winners, max_tokens=args.max_tokens, max_queries=args.max_queries)

    if args.out:

        out_path = Path(args.out)

    else:

        safe = re.sub(r"[^A-Za-z0-9]+", "_", str(args.topic)).strip("_") or "topic"

        out_path = Path("configs/categories/_generated") / f"{safe}.yaml"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    out_text = yaml.safe_dump(cfg, sort_keys=False, allow_unicode=True)
    out_path.write_text(out_text, encoding="utf-8")

    logger.info("Wrote topic YAML to %s", str(out_path))


if __name__ == "__main__":
    main()

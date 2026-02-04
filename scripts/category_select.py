#!/usr/bin/env python3
"""
scripts/category_select.py

Replacement that implements:
 - select_topics(user_query_or_seed_keywords, seed_article_ids_or_files, runtime_mode)
 - CLI preserving old interface: --mode, --out, etc.

Outputs JSON lines or JSON array to stdout/file.

Requirements (runtime):
 - python3.9+
 - numpy, scikit-learn, nltk (optional), sentence_transformers (optional)
 - requests (for OpenRouter/Ollama), openai (optional)
"""

from __future__ import annotations
import argparse
import hashlib
import json
import logging
import os
import re
import sys
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path

# Local helper (LLM wrapper)
from gen_category_from_llm import LLMClient, canned_llm_response_for_dry_run

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("category_select")

# --- Utilities ---
def slugify(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text[:40] or "cat"

def stable_id(label: str, mode: str) -> str:
    # deterministic canonical id: slug + short stable hash of (label + mode)
    s = f"{label}||{mode}"
    h = hashlib.sha1(s.encode("utf-8")).hexdigest()[:8]
    return f"{slugify(label)}-{h}"

def sanitize_text(s: str) -> str:
    if s is None:
        return ""
    return s.encode("utf-8", errors="replace").decode("utf-8", errors="replace")

# --- Deterministic pipeline (TF-IDF + KMeans) ---
def deterministic_topics_from_texts(
    texts: List[str],
    k: int = 12,
    seed_boost_indexes: Optional[List[int]] = None,
) -> List[Dict[str, Any]]:
    """
    Build topics using TF-IDF + KMeans. Returns list of candidate dicts.
    seed_boost_indexes: indices in `texts` that should be weighted more heavily.
    """
    import numpy as np
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.cluster import KMeans

    clean_texts = [re.sub(r"\s+", " ", t.strip()) for t in texts]
    if not any(clean_texts):
        return []

    vectorizer = TfidfVectorizer(max_df=0.8, min_df=1, ngram_range=(1,2), max_features=5000)
    X = vectorizer.fit_transform(clean_texts)

    # seed-boost: simple approach - duplicate seed rows to bias clustering
    if seed_boost_indexes:
        X_list = [X]
        for idx in seed_boost_indexes:
            if 0 <= idx < X.shape[0]:
                # add the same vector multiple times (weight 3)
                for _ in range(2):
                    X_list.append(X[idx])
        X = np.vstack([x.toarray() if hasattr(x, "toarray") else x for x in X_list])

    # run KMeans
    k = min(k, max(1, X.shape[0]))
    km = KMeans(n_clusters=k, random_state=42, n_init=10)
    km.fit(X)
    centers = km.cluster_centers_
    terms = vectorizer.get_feature_names_out()

    # For each cluster, pick top terms
    from numpy import argsort
    results = []
    for i, center in enumerate(centers):
        top_idx = list(argsort(center)[-6:][::-1])
        top_terms = [terms[t] for t in top_idx if t < len(terms)]
        label = " ".join(top_terms[:3]) or f"topic-{i}"
        canonical = f"{i}-{slugify('-'.join(top_terms))}"
        description = f"Cluster {i} — top terms: {', '.join(top_terms[:6])}"
        # find representative docs (closest to center)
        # compute cosine distance on dense arrays
        import numpy.linalg as la
        arrs = X if isinstance(X, (list, tuple)) else X
        # when we duplicated seed rows, arrs is dense; convert to dense matrix
        if hasattr(X, "shape") and len(X.shape) == 2:
            dense = X
        else:
            dense = X
        # fallback: choose the original doc with same cluster label via km.predict if shapes align
        # We compute using the original km.predict on the original matrix (re-fit if needed)
        try:
            # re-predict on original docs
            original_km = KMeans(n_clusters=k, random_state=42, n_init=10)
            original_km.fit(vectorizer.transform(clean_texts))
            labels = original_km.labels_
            doc_idxs = [j for j, lbl in enumerate(labels) if lbl == i]
        except Exception:
            doc_idxs = []
        examples = [str(idx) for idx in (doc_idxs[:3] if doc_idxs else [0])]
        results.append({
            "label": sanitize_text(label),
            "canonical_id": sanitize_text(canonical),
            "description": sanitize_text(description),
            "examples": examples,
            "confidence": 0.6,
            "generation_mode": "deterministic",
        })
    return results

# --- Hybrid pipeline ---
def hybrid_topics(texts: List[str], llm_client: LLMClient, m: int = 20, **llm_kwargs) -> List[Dict[str, Any]]:
    # produce M deterministic candidates then refine with LLM
    det = deterministic_topics_from_texts(texts, k=min(24, max(4, m)))
    candidates = [d["label"] for d in det]
    # prepare LLM prompt: ask to refine these candidates, merge near-duplicates, rank, and add short desc
    prompt = {
        "instruction": (
            "You are given candidate topic labels (from a classical NLP run). "
            "Do NOT invent unconstrained categories. Refine, merge near-duplicates, "
            "rank them by importance to the user's query, remove obvious duplicates, "
            "and return up to 12 categories. Output JSON array with fields: "
            "[label, description, example_excerpt (optional), confidence]. Return JSON only."
        ),
        "candidates": candidates
    }
    response = llm_client.call_llm_for_categories(prompt=prompt, **llm_kwargs)
    # validation + stable ids
    out = []
    for item in response:
        label = sanitize_text(item.get("label","")).strip()
        if not label:
            continue
        out.append({
            "label": label,
            "canonical_id": stable_id(label, "hybrid"),
            "description": sanitize_text(item.get("description","")),
            "examples": item.get("examples", []) or [],
            "confidence": float(item.get("confidence", 0.6)),
            "generation_mode": "hybrid",
        })
    return out

# --- LLM mode ---
def llm_topics(user_query: str, llm_client: LLMClient, n: int = 12, max_tokens: int = 1024, max_queries: int = 1, **kwargs) -> List[Dict[str, Any]]:
    # Build concise prompt asking for JSON only
    prompt = {
        "instruction": (
            "Given the user seed or query, produce up to %(n)s candidate categories.\n"
            "For each candidate produce: label, canonical_id (do not invent random ids), "
            "short_description (1-2 sentences), example_article_excerpt (one short excerpt or doc id), "
            "confidence_score (0-1 float).\n"
            "Return JSON array only. No prose. Use ASCII/UTF-8 safe text." % {"n": n}
        ),
        "user_query": user_query
    }
    # Respect limits inside LLM client
    response = llm_client.call_llm_for_categories(prompt=prompt, n=n, max_tokens=max_tokens, max_queries=max_queries, **kwargs)
    # validate and sanitize
    out = []
    for entry in response:
        label = sanitize_text(entry.get("label","")).strip()
        if not label:
            continue
        canonical = entry.get("canonical_id") or stable_id(label, "llm")
        canonical = sanitize_text(canonical)
        out.append({
            "label": label,
            "canonical_id": canonical,
            "description": sanitize_text(entry.get("short_description","")),
            "examples": entry.get("examples", []) or [entry.get("example_article_excerpt","")] if entry.get("example_article_excerpt") else [],
            "confidence": float(entry.get("confidence_score", entry.get("confidence", 0.5))),
            "generation_mode": "llm",
        })
    return out

# --- Top-level API ---
def select_topics(
    user_query_or_seed_keywords: str,
    seed_article_paths_or_ids: Optional[List[str]] = None,
    runtime_mode: str = "hybrid",
    out_path: Optional[str] = None,
    dry_run: bool = False,
    k_det: int = 12,
    m_hybrid: int = 20,
    max_llm_tokens: int = 1024,
    max_llm_queries: int = 1,
    fallback_to_deterministic: bool = True,
) -> List[Dict[str, Any]]:
    """
    Primary function.
    """
    # Load texts from seed_article_paths_or_ids (if any)
    texts = []
    seed_indexes = []
    if seed_article_paths_or_ids:
        for idx, p in enumerate(seed_article_paths_or_ids):
            try:
                if os.path.exists(p):
                    texts.append(Path(p).read_text(encoding="utf-8"))
                    seed_indexes.append(len(texts)-1)
                else:
                    # treat as doc id placeholder
                    texts.append(str(p))
            except Exception as e:
                logger.warning("Couldn't read %s: %s", p, e)
    # always include the user query as a short "document" to help deterministic methods
    if user_query_or_seed_keywords:
        texts = [user_query_or_seed_keywords] + texts

    llm_client = LLMClient(dry_run=dry_run)

    try:
        if runtime_mode == "deterministic":
            results = deterministic_topics_from_texts(texts, k=k_det, seed_boost_indexes=seed_indexes)
        elif runtime_mode == "llm":
            results = llm_topics(user_query_or_seed_keywords, llm_client, n=k_det, max_tokens=max_llm_tokens, max_queries=max_llm_queries)
        elif runtime_mode == "hybrid":
            results = hybrid_topics(texts, llm_client, m=m_hybrid, max_tokens=max_llm_tokens, max_queries=max_llm_queries)
        else:
            raise ValueError("unknown mode")
    except Exception as e:
        logger.exception("Error during topic generation: %s", e)
        if fallback_to_deterministic:
            logger.warning("Falling back to deterministic mode")
            results = deterministic_topics_from_texts(texts, k=k_det, seed_boost_indexes=seed_indexes)
        else:
            raise

    # canonical_id enforcement: ensure deterministic canonical ids
    for r in results:
        if "canonical_id" not in r or not r["canonical_id"]:
            r["canonical_id"] = stable_id(r["label"], r.get("generation_mode", runtime_mode))

    # Write output
    if out_path:
        outp = Path(out_path)
        outp.write_text(json.dumps(results, indent=2), encoding="utf-8")
        logger.info("Wrote %d categories to %s", len(results), out_path)
    return results

# --- CLI ---
def main(argv=None):
    p = argparse.ArgumentParser(prog="category_select.py")
    p.add_argument("--mode", choices=["llm", "hybrid", "deterministic"], default="hybrid")
    p.add_argument("--query", required=False, default="")
    p.add_argument("--seed", nargs="*", default=[])
    p.add_argument("--k", type=int, default=12, help="k for deterministic or max categories for LLM")
    p.add_argument("--out", required=False, default=None)
    p.add_argument("--dry-run", action="store_true", help="simulate LLM calls")
    p.add_argument("--max-llm-tokens", type=int, default=1024)
    p.add_argument("--max-llm-queries", type=int, default=1)
    p.add_argument("--fallback-to-deterministic", action="store_true", default=True)
    args = p.parse_args(argv)

    res = select_topics(
        user_query_or_seed_keywords=args.query,
        seed_article_paths_or_ids=args.seed,
        runtime_mode=args.mode,
        out_path=args.out,
        dry_run=args.dry_run,
        k_det=args.k,
        m_hybrid=args.k,
        max_llm_tokens=args.max_llm_tokens,
        max_llm_queries=args.max_llm_queries,
        fallback_to_deterministic=args.fallback_to_deterministic,
    )
    # print to stdout as JSON array
    print(json.dumps(res, indent=2))

if __name__ == "__main__":
    main()

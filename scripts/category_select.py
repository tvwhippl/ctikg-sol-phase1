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
try:
    from scripts.gen_category_from_llm import LLMClient, canned_llm_response_for_dry_run
except Exception:
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
    Build topics using TF-IDF + KMeans, robust to tiny corpora.
    - Adjusts TF-IDF parameters for small n_docs.
    - Falls back to simple heuristic if TF-IDF/KMeans can't run.
    """
    import numpy as np
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.cluster import KMeans

    # Clean texts and remove empty documents
    clean_texts = [re.sub(r"\\s+", " ", (t or "").strip()) for t in texts]
    clean_texts = [t for t in clean_texts if t]
    n_docs = max(0, len(clean_texts))
    if n_docs == 0:
        return []

    # Adjust TF-IDF params based on corpus size to avoid scikit errors
    if n_docs == 1:
        max_df = 1.0
        min_df = 1
        max_features = 500
    elif n_docs == 2:
        # set max_df high enough so max_df * n_docs >= min_df (1)
        max_df = 1.0
        min_df = 1
        max_features = 1000
    else:
        max_df = 0.8
        min_df = 1
        max_features = min(5000, max(1000, n_docs * 50))

    vectorizer = TfidfVectorizer(max_df=max_df, min_df=min_df, ngram_range=(1,2), max_features=max_features)
    try:
        X = vectorizer.fit_transform(clean_texts)
    except Exception as e:
        # Fallback: build trivial topics from each document's top tokens
        logger.warning("TF-IDF failed (%s). Falling back to simple token heuristic.", e)
        results = []
        for idx, doc in enumerate(clean_texts):
            tokens = [t for t in re.findall(r'\\w+', doc.lower()) if len(t) > 3]
            top_terms = tokens[:3] or [doc[:20]]
            label = " ".join(top_terms)
            canonical = f"{idx}-{slugify('-'.join(top_terms))}"
            results.append({
                "label": sanitize_text(label),
                "canonical_id": sanitize_text(canonical),
                "description": f"Fallback topic from document {idx}",
                "examples": [str(idx)],
                "confidence": 0.5,
                "generation_mode": "deterministic",
            })
        return results

    # If vectorizer produced no features, fallback similarly
    if X.shape[1] == 0:
        logger.warning("TF-IDF produced zero features; using fallback.")
        results = []
        for idx, doc in enumerate(clean_texts):
            tokens = [t for t in re.findall(r'\\w+', doc.lower()) if len(t) > 3]
            top_terms = tokens[:3] or [doc[:20]]
            label = " ".join(top_terms)
            canonical = f"{idx}-{slugify('-'.join(top_terms))}"
            results.append({
                "label": sanitize_text(label),
                "canonical_id": sanitize_text(canonical),
                "description": f"Fallback topic from document {idx}",
                "examples": [str(idx)],
                "confidence": 0.5,
                "generation_mode": "deterministic",
            })
        return results

    # seed-boost: duplicate seed vectors to bias clustering
    if seed_boost_indexes:
        X_list = [X.toarray()]
        for idx in seed_boost_indexes:
            if 0 <= idx < X.shape[0]:
                # repeat the vector a couple times to bias KMeans
                for _ in range(2):
                    X_list.append(X[idx].toarray())
        Xmat = np.vstack(X_list)
    else:
        Xmat = X.toarray()

    # Ensure k <= n_docs (KMeans cannot have more clusters than samples)
    k = max(1, min(k, Xmat.shape[0]))

    # Use MiniBatchKMeans for larger n_docs, otherwise KMeans
    try:
        if Xmat.shape[0] > 300:
            from sklearn.cluster import MiniBatchKMeans as MBK
            km = MBK(n_clusters=k, random_state=42)
        else:
            km = KMeans(n_clusters=k, random_state=42, n_init=10)
        km.fit(Xmat)
        centers = km.cluster_centers_
    except Exception as e:
        logger.warning("KMeans failed (%s); using single-cluster fallback.", e)
        centers = [Xmat.mean(axis=0)]

    terms = vectorizer.get_feature_names_out()

    from numpy import argsort
    results = []
    # Refit a KMeans on the original docs if shapes differ
    try:
        labels = km.predict(X.toarray()) if hasattr(km, "predict") else [0]*n_docs
    except Exception:
        labels = [0]*n_docs

    for i, center in enumerate(centers):
        top_idx = list(argsort(center)[-6:][::-1])
        top_terms = [terms[t] for t in top_idx if t < len(terms)]
        # Clean top_terms: split ngrams to tokens, remove stopwords, short tokens, dedupe
        try:
            from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
            stopset = set(ENGLISH_STOP_WORDS)
        except Exception:
            stopset = set()
        cleaned_tokens = []
        seen = set()
        for term in top_terms:
            # split term into simple tokens (keep alphanum)
            for tok in __import__('re').findall(r"\w+", term.lower()):
                if tok in seen:
                    continue
                if tok in stopset:
                    continue
                if len(tok) < 3:
                    continue
                cleaned_tokens.append(tok)
                seen.add(tok)
        # prefer cleaned tokens otherwise fall back to ngram join
        if cleaned_tokens:
            label = " ".join(cleaned_tokens[:3])
        else:
            label = " ".join(top_terms[:3]) if top_terms else f"topic-{i}"
        # canonical id: human-friendly slug + stable short hash
        canonical = stable_id(label, 'deterministic')
        description = f"Cluster {i} — top terms: {', '.join(cleaned_tokens[:6] or top_terms[:6])}"
        # representative docs for this cluster
        doc_idxs = [j for j, lbl in enumerate(labels) if lbl == i]
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

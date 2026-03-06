#!/usr/bin/env python3
"""scripts/category_select.py

Legacy/open-topic **winner selection** script.

This file is intentionally kept compatible with the open_topic.mk interface:

  python3 scripts/category_select.py --in data/Links_Queue_sorted_flags.csv --category <yaml>

It reads:
  - a link queue CSV (from pre_rank_links_v3 + make_helper_flags)
  - a category YAML with schema documented in docs/TOPICS.md:
        name: <string>
        include: ["kw1", "kw2", ...]
        exclude: ["kwA", ...]
        winners: <int>

It writes:
  data/Selected_<SAFE_NAME>.csv

Where SAFE_NAME matches the Makefile logic:
  re.sub(r'[^A-Za-z0-9]+','_', name).strip('_')

Note:
- This is *not* the topic-candidate generation script. That lives in
  scripts/topic_candidate_select.py.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("category_select")


def _safe_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", name or "").strip("_") or "Category"


def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize common column names.

    Important: the link queue sometimes contains both lowercase and uppercase
    variants (e.g., `url` and `URL`). This helper will **prefer** the canonical
    column if it already exists, and will only rename/fill/drop variants in a
    way that avoids creating duplicate column labels.
    """

    df = df.copy()

    def merge_variant(canonical: str, variants: List[str]) -> None:
        nonlocal df
        existing = [c for c in [canonical] + variants if c in df.columns]
        if not existing:
            return

        if canonical not in df.columns:
            # Rename first available variant to canonical
            for v in variants:
                if v in df.columns:
                    df = df.rename(columns={v: canonical})
                    break

        # If canonical exists and variants exist, fill missing values then drop variants
        for v in variants:
            if v in df.columns and v != canonical:
                try:
                    df[canonical] = df[canonical].where(df[canonical].notna(), df[v])
                except Exception:
                    pass
                # Drop the variant to avoid duplicate labels later
                df = df.drop(columns=[v])

    # Canonical merges
    merge_variant("URL", ["url", "Url", "link", "Link"])
    merge_variant("Title", ["title", "headline", "Headline"])
    merge_variant("Snippet", ["snippet", "summary", "description", "Summary", "Description"])
    merge_variant("Source_Domain", ["source_domain", "domain", "Domain", "SourceDomain"])
    merge_variant("Score", ["score", "Score"])
    merge_variant("Quality4", ["quality4", "Quality4"])
    merge_variant("Quality2", ["quality2", "Quality2"])
    merge_variant("RepFlag", ["repflag", "Repflag", "RepFlag"])
    merge_variant("SigFlag", ["sigflag", "Sigflag", "SigFlag"])
    merge_variant("Status", ["status", "Status"])

    return df



def _coerce_numeric(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df


def _count_hits(text: str, terms: List[str]) -> int:
    if not text or not terms:
        return 0
    t = text.lower()
    hits = 0
    for term in terms:
        term = (term or "").strip().lower()
        if not term:
            continue
        # Default: simple substring match.
        #
        # Special-case short alphanumeric tokens (e.g., "rce", "ssrf", "xss")
        # to avoid false positives like "rce" matching the "...souRCE" suffix.
        if term.isalnum() and len(term) <= 4:
            if re.search(rf"\b{re.escape(term)}\b", t):
                hits += 1
        else:
            if term in t:
                hits += 1
    return hits

def _tfidf_query_sim(query: str, docs: List[str]) -> List[float]:
    """
    Return cosine-similarity-like scores (TF-IDF dot product with L2 norm)
    between query and each doc. Robust fallback when include terms miss.

    Safe behavior: on any failure, return all-zeros.
    """
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        import numpy as np

        query = (query or "").strip()
        if not query or not docs:
            return [0.0] * len(docs)

        # Fit on [query + docs] so vocab covers both
        vec = TfidfVectorizer(stop_words="english", ngram_range=(1, 2), max_features=8000)
        X = vec.fit_transform([query] + docs)
        q = X[0]
        D = X[1:]

        # With default norm='l2', dot product approximates cosine similarity
        sims = (D @ q.T).toarray().ravel()
        sims = np.nan_to_num(sims, nan=0.0, posinf=0.0, neginf=0.0)
        return [float(x) for x in sims]
    except Exception:
        return [0.0] * len(docs)

def _format_out(df: pd.DataFrame, *, topic_name: str, fallback_used: bool, pool: str) -> pd.DataFrame:
    """Format ranked/selected outputs with stable, scraper-friendly columns.

    ranked.csv is for debugging/determinism; selected.csv is the slice used for scraping.
    """
    for c in ["Score", "Quality4", "Quality2", "RepFlag", "SigFlag", "Status"]:
        if c not in df.columns:
            df[c] = 0

    if "__rank" not in df.columns:
        df["__rank"] = list(range(1, len(df) + 1))

    out_df = pd.DataFrame(
        {
            "Rank": df.get("__rank", 0),
            "URL": df.get("URL", ""),
            "Title": df.get("Title", ""),
            "Source_Domain": df.get("Source_Domain", ""),
            "Category": topic_name,
            "Score": df.get("Score", 0),
            "Quality4": df.get("Quality4", 0),
            "Quality2": df.get("Quality2", 0),
            "RepFlag": df.get("RepFlag", 0),
            "SigFlag": df.get("SigFlag", 0),
            "Status": df.get("Status", ""),
            "TopicScore": df.get("__topic_score", 0),
            "IncHits": df.get("__inc_hits", 0),
            "ExcHits": df.get("__exc_hits", 0),
            "QuerySim": df.get("__qsim", 0.0),
            "CandidatePool": pool,
            "FallbackUsed": fallback_used,
        }
    )
    return out_df


def _select_winners(
    df: pd.DataFrame,
    cat: Dict[str, Any],
    selected_path: str,
    *,
    ranked_path: str | None = None,
    scrape_max: int | None = None,
    offset: int = 0,
    fill_to_winners: bool = False,
    min_qsim: float = 0.0,
) -> Tuple[pd.DataFrame, bool, int, int]:
    name = str(cat.get("name") or "Topic").strip()
    include = [str(x).strip() for x in (cat.get("include") or []) if str(x).strip()]
    exclude = [str(x).strip() for x in (cat.get("exclude") or []) if str(x).strip()]

    winners_yaml = int(cat.get("winners") or 100)
    winners_yaml = max(1, winners_yaml)

    cap = int(scrape_max) if scrape_max is not None else winners_yaml
    cap = max(1, cap)

    offset = int(offset or 0)
    offset = max(0, offset)

    df = _normalize_cols(df)
    df = _coerce_numeric(df, ["Score", "Quality4", "Quality2", "RepFlag", "SigFlag"])
    df["__text"] = (df["Title"].fillna("") + "\n" + df["Snippet"].fillna("")).astype(str).str.lower()

    df["__inc_hits"] = df["__text"].apply(lambda t: _count_hits(t, include))
    df["__exc_hits"] = df["__text"].apply(lambda t: _count_hits(t, exclude)) if exclude else 0
    df["__topic_score"] = df["__inc_hits"] - (2 * df["__exc_hits"])
    df["__qsim"] = 0.0

    strict_mask = (df["__inc_hits"] > 0) & (df["__exc_hits"] == 0)
    strict_empty = not bool(strict_mask.any())

    need_qsim = strict_empty or fill_to_winners or (ranked_path is not None)
    if need_qsim:
        query = " ".join([name] + include).strip()
        df["__qsim"] = _tfidf_query_sim(query, df["__text"].tolist())

    cand = df[strict_mask].copy()
    fallback_used = False
    pool = "strict"

    if cand.empty:
        fallback_used = True
        pool = "fallback_qsim"
        cand = df[(df["__qsim"] >= float(min_qsim)) & (df["__exc_hits"] == 0)].copy()
        if cand.empty:
            pool = "fallback_exclude_only"
            cand = df[df["__exc_hits"] == 0].copy()

    cand.sort_values(
        by=["__topic_score", "__qsim", "Quality4", "Quality2", "Score", "URL"],
        ascending=[False, False, False, False, False, True],
        inplace=True,
        kind="mergesort",
    )

    ranked = cand.drop_duplicates(subset=["URL"]).copy().reset_index(drop=True)
    ranked["__rank"] = list(range(1, len(ranked) + 1))

    if fill_to_winners and not strict_empty and len(ranked) < cap:
        need = cap - len(ranked)
        remaining = df[~df["URL"].isin(ranked["URL"])].copy()
        remaining = remaining[remaining["__exc_hits"] == 0]
        remaining.sort_values(
            by=["__qsim", "Quality4", "Quality2", "Score", "URL"],
            ascending=[False, False, False, False, True],
            inplace=True,
            kind="mergesort",
        )
        fill = (
            remaining[remaining["__qsim"] >= float(min_qsim)]
            .drop_duplicates(subset=["URL"])
            .head(need)
            .copy()
        )
        if len(fill) > 0:
            fallback_used = True
            pool = "strict+fill"
            ranked = pd.concat([ranked, fill], ignore_index=True).drop_duplicates(subset=["URL"]).reset_index(drop=True)
            ranked["__rank"] = list(range(1, len(ranked) + 1))

    selected = ranked.iloc[offset : offset + cap].copy()

    if ranked_path:
        Path(ranked_path).parent.mkdir(parents=True, exist_ok=True)
        _format_out(ranked, topic_name=name, fallback_used=fallback_used, pool=pool).to_csv(ranked_path, index=False)

    Path(selected_path).parent.mkdir(parents=True, exist_ok=True)
    out_selected = _format_out(selected, topic_name=name, fallback_used=fallback_used, pool=pool)
    out_selected.to_csv(selected_path, index=False)

    return out_selected, fallback_used, cap, len(ranked)

def run(
    in_path: str,
    category_yaml_path: str,
    out_path: str | None = None,
    *,
    ranked_path: str | None = None,
    selected_path: str | None = None,
    scrape_max: int | None = None,
    offset: int = 0,
    fill_to_winners: bool = False,
    min_qsim: float = 0.0,
) -> str:
    """Run selection and return selected CSV path."""
    cat = yaml.safe_load(Path(category_yaml_path).read_text(encoding="utf-8"))
    if not isinstance(cat, dict):
        raise ValueError(f"Category YAML must be a mapping/object: {category_yaml_path}")

    name = str(cat.get("name") or "").strip() or "Category"

    winners_int = 100
    if "winners" in cat:
        try:
            winners_int = int(cat.get("winners"))
        except Exception:
            winners_int = 100

    if selected_path is None:
        selected_path = out_path

    safe = _safe_name(name)
    if selected_path is None:
        out_dir = Path(in_path).parent
        selected_path = str(out_dir / f"Selected_{safe}.csv")

    df = pd.read_csv(in_path)

    out_df, fallback_used, cap, ranked_total = _select_winners(
        df,
        cat=cat,
        selected_path=str(selected_path),
        ranked_path=ranked_path,
        scrape_max=scrape_max,
        offset=int(offset or 0),
        fill_to_winners=bool(fill_to_winners),
        min_qsim=float(min_qsim),
    )

    meta = f"yaml_winners={winners_int}" if scrape_max is not None else ""
    logger.info(
        "Selected %s/%s for '%s' offset=%s ranked_total=%s (fallback_used=%s) %s -> %s",
        len(out_df),
        cap,
        name,
        int(offset or 0),
        ranked_total,
        fallback_used,
        meta,
        selected_path,
    )
    return str(selected_path)
def main() -> None:

    ap = argparse.ArgumentParser(description="Select winners for a generated category YAML")
    ap.add_argument("--in", dest="in_path", required=True, help="Input queue CSV (e.g., data/Links_Queue_sorted_flags.csv)")
    ap.add_argument("--category", required=True, help="Category YAML path (name/include/exclude/winners)")

    ap.add_argument("--out", default=None, help="(deprecated) Selected output CSV path (alias for --selected-out)")
    ap.add_argument("--selected-out", default=None, help="Selected output CSV path (slice used for scraping)")
    ap.add_argument("--ranked-out", default=None, help="Ranked output CSV path (full ranked candidates)")

    ap.add_argument("--scrape-max", type=int, default=None, help="Operational cap for selected slice (overrides YAML winners)")
    ap.add_argument("--offset", type=int, default=0, help="Offset into ranked list for pagination (default: 0)")

    ap.add_argument(
        "--fill-to-winners",
        action="store_true",
        help="If set, pad selection up to the operational cap using semantic similarity (lower precision).",
    )
    ap.add_argument(
        "--min-qsim",
        type=float,
        default=0.0,
        help="Minimum TF-IDF similarity score for semantic fallback/fill. Default 0.0 (legacy).",
    )
    args = ap.parse_args()

    run(
        args.in_path,
        args.category,
        out_path=args.out,
        ranked_path=args.ranked_out,
        selected_path=args.selected_out,
        scrape_max=args.scrape_max,
        offset=int(args.offset or 0),
        fill_to_winners=bool(args.fill_to_winners),
        min_qsim=float(args.min_qsim),
    )


if __name__ == "__main__":
    main()

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

def _select_winners(
    df: pd.DataFrame,
    cat: Dict[str, Any],
    out_path: str,
    *,
    fill_to_winners: bool = False,
    min_qsim: float = 0.0,
) -> Tuple[pd.DataFrame, bool]:
    name = str(cat.get("name") or "Topic").strip()
    include = [str(x).strip() for x in (cat.get("include") or []) if str(x).strip()]
    exclude = [str(x).strip() for x in (cat.get("exclude") or []) if str(x).strip()]
    winners = int(cat.get("winners") or 100)
    winners = max(1, winners)

    df = _normalize_cols(df)
    df["__text"] = (df["Title"].fillna("") + "\n" + df["Snippet"].fillna("")).astype(str).str.lower()

    # Primary scoring: include/exclude hits
    df["__inc_hits"] = df["__text"].apply(lambda t: _count_hits(t, include))
    df["__exc_hits"] = df["__text"].apply(lambda t: _count_hits(t, exclude)) if exclude else 0
    df["__topic_score"] = df["__inc_hits"] - (2 * df["__exc_hits"])

    # Always have these columns, even if we don't compute them yet
    df["__qsim"] = 0.0

    # Strict candidate set: include hits > 0 and no exclude hits
    #
    # IMPORTANT behavioral choice (for scaling):
    # - If we have *any* strict matches, we prefer to **underfill** rather than
    #   pad to `winners` with weak semantic matches.
    # - We only use semantic fallback to *rescue* the case where strict matches
    #   are empty, or when `fill_to_winners=True`.
    cand_strict = df[(df["__inc_hits"] > 0) & (df["__exc_hits"] == 0)].copy()
    cand = cand_strict
    fallback_used = False

    # Compute TF-IDF similarity only when needed.
    need_qsim = cand.empty or fill_to_winners
    if need_qsim:
        query = " ".join([name] + include).strip()
        df["__qsim"] = _tfidf_query_sim(query, df["__text"].tolist())

    # If strict match set is empty, fall back to TF-IDF similarity.
    if cand.empty:
        fallback_used = True
        # Prefer rows that have *some* semantic match to the topic.
        # `min_qsim` defaults to 0.0 (legacy behavior), but can be raised to reduce noise.
        cand = df[(df["__qsim"] >= float(min_qsim)) & (df["__exc_hits"] == 0)].copy()

        # If similarity yields nothing (e.g., all zeros), revert to exclude-only.
        if cand.empty:
            cand = df[df["__exc_hits"] == 0].copy()

    # Ranking: topic score first, then similarity, then existing quality/score fields
    sort_cols = ["__topic_score", "__qsim", "Quality4", "Quality2", "Score"]
    # Ensure missing sort cols exist
    for c in sort_cols:
        if c not in cand.columns:
            cand[c] = 0

    cand.sort_values(
        by=sort_cols,
        ascending=[False, False, False, False, False],
        inplace=True,
        kind="mergesort",
    )

    # Dedupe by URL; take top winners
    selected = cand.drop_duplicates(subset=["URL"]).head(winners).copy()

    # Optional: fill to `winners` using similarity-ranked remaining rows.
    # Default is OFF to avoid low-precision padding during large batch runs.
    if fill_to_winners and len(selected) < winners:
        need = winners - len(selected)
        remaining = df[~df["URL"].isin(selected["URL"])].copy()
        remaining = remaining[remaining["__exc_hits"] == 0]
        # qsim already computed above when fill_to_winners=True
        remaining.sort_values(
            by=["__qsim", "Quality4", "Quality2", "Score"],
            ascending=[False, False, False, False],
            inplace=True,
            kind="mergesort",
        )
        fill = (
            remaining[remaining["__qsim"] >= float(min_qsim)]
            .drop_duplicates(subset=["URL"])
            .head(need)
        )
        if len(fill) > 0:
            fallback_used = True
            # Mark these as fallback rows.
            fill = fill.copy()
            fill["__is_fallback_row"] = True
            selected["__is_fallback_row"] = False
            selected = pd.concat([selected, fill], ignore_index=True)
        else:
            selected["__is_fallback_row"] = False
    else:
        selected["__is_fallback_row"] = False

    # Output with debug columns so you can see WHY a row was selected
    out_df = pd.DataFrame(
        {
            "URL": selected["URL"],
            "Title": selected["Title"],
            "Source_Domain": selected["Source_Domain"],
            "Category": name,
            "Score": selected.get("Score", 0),
            "TopicScore": selected.get("__topic_score", 0),
            "IncHits": selected.get("__inc_hits", 0),
            "ExcHits": selected.get("__exc_hits", 0),
            "QuerySim": selected.get("__qsim", 0.0),
            "IsFallbackRow": selected.get("__is_fallback_row", False),
            "FallbackUsed": fallback_used,
        }
    )
    out_df.to_csv(out_path, index=False)
    return out_df, fallback_used


def run(
    in_path: str,
    category_yaml_path: str,
    out_path: str | None = None,
    *,
    fill_to_winners: bool = False,
    min_qsim: float = 0.0,
) -> str:
    """Run selection and return output CSV path.

    This is the entrypoint used by open_topic.mk (topic-select) and unit tests.
    """
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

    # output path default: alongside the queue CSV
    safe = _safe_name(name)
    if out_path is None:
        out_dir = Path(in_path).parent
        out_path = str(out_dir / f"Selected_{safe}.csv")

    # load + normalize
    df = pd.read_csv(in_path)
    df = _normalize_cols(df)

    # select + write
    out_df, fallback_used = _select_winners(
        df,
        cat=cat,
        out_path=out_path,
        fill_to_winners=bool(fill_to_winners),
        min_qsim=float(min_qsim),
    )

    logger.info(
        "Selected %s/%s winners for '%s' (fallback_used=%s) -> %s",
        len(out_df),
        winners_int,
        name,
        fallback_used,
        out_path,
    )
    return out_path


def main() -> None:

    ap = argparse.ArgumentParser(description="Select winners for a generated category YAML")
    ap.add_argument("--in", dest="in_path", required=True, help="Input queue CSV (e.g., data/Links_Queue_sorted_flags.csv)")
    ap.add_argument("--category", required=True, help="Category YAML path (name/include/exclude/winners)")
    ap.add_argument("--out", default=None, help="Optional explicit output CSV path")
    ap.add_argument(
        "--fill-to-winners",
        action="store_true",
        help="If set, pad selection up to `winners` using semantic similarity (lower precision).",
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
        fill_to_winners=bool(args.fill_to_winners),
        min_qsim=float(args.min_qsim),
    )


if __name__ == "__main__":
    main()

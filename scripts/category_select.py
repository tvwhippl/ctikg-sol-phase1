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
        # simple substring match; robust and cheap
        if term in t:
            hits += 1
    return hits


def _select_winners(
    df: pd.DataFrame,
    name: str,
    include: List[str],
    exclude: List[str],
    winners: int,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Return (selected_df, stats). Never returns empty if df is non-empty."""

    df = df.copy()

    # Basic filtering: ignore explicit rejects if Status exists
    if "Status" in df.columns:
        df = df[df["Status"].fillna("").str.lower() != "rejected"].copy()

    if df.empty:
        return df, {"selected": 0, "reason": "input_empty_after_filter"}

    # Build search text
    title = df["Title"] if "Title" in df.columns else ""
    snippet = df["Snippet"] if "Snippet" in df.columns else ""
    df["__text"] = (title.fillna("").astype(str) + "\n" + snippet.fillna("").astype(str)).str.lower()

    include = [t for t in (include or []) if str(t).strip()]
    exclude = [t for t in (exclude or []) if str(t).strip()]

    df["__inc_hits"] = df["__text"].apply(lambda s: _count_hits(s, include))
    df["__exc_hits"] = df["__text"].apply(lambda s: _count_hits(s, exclude))

    # Topic score: include hits minus a heavier penalty for exclude hits
    df["__topic_score"] = df["__inc_hits"] - (2 * df["__exc_hits"])

    # Candidate filter
    used_fallback = False
    cand = df
    if include:
        cand = df[df["__inc_hits"] > 0].copy()
        if cand.empty:
            # fallback: if include list is too strict, take the best overall by quality
            used_fallback = True
            cand = df.copy()

    # numeric coercion for sort keys
    cand = _coerce_numeric(cand, ["Quality4", "Quality2", "RepFlag", "SigFlag", "Score"])

    # sort keys (topic_score first)
    sort_cols = ["__topic_score", "Quality4", "Quality2", "RepFlag", "SigFlag", "Score"]
    sort_cols = [c for c in sort_cols if c in cand.columns]
    ascending = [False] * len(sort_cols)

    cand = cand.sort_values(sort_cols, ascending=ascending, kind="mergesort")

    # de-dupe by URL if present
    url_col = "URL" if "URL" in cand.columns else None
    if url_col:
        cand = cand.drop_duplicates(subset=[url_col], keep="first")

    selected = cand.head(max(0, int(winners))).copy()

    stats = {
        "category": name,
        "winners_requested": int(winners),
        "selected": int(len(selected)),
        "had_include_terms": bool(include),
        "fallback_used": used_fallback,
        "total_candidates": int(len(cand)),
    }
    return selected, stats


def run(in_path: str, category_yaml_path: str, out_path: str | None = None) -> str:
    """Run selection and return output CSV path."""
    cat = yaml.safe_load(Path(category_yaml_path).read_text(encoding="utf-8"))
    if not isinstance(cat, dict):
        raise ValueError(f"Category YAML must be a mapping/object: {category_yaml_path}")

    name = str(cat.get("name") or "").strip() or "Category"
    include = cat.get("include") or []
    exclude = cat.get("exclude") or []

    winners_int = 100
    if "winners" in cat:
        try:
            winners_int = int(cat.get("winners"))
        except Exception:
            winners_int = 100

    # load CSV
    df = pd.read_csv(in_path)
    df = _normalize_cols(df)

    selected, stats = _select_winners(df, name=name, include=list(include), exclude=list(exclude), winners=winners_int)

    safe = _safe_name(name)
    if out_path is None:
        out_dir = str(Path(in_path).parent)
        out_path = str(Path(out_dir) / f"Selected_{safe}.csv")

    # Build output frame expected by scrape_selected.py
    if selected.empty:
        out_df = pd.DataFrame(columns=["URL", "Title", "Source_Domain", "Category", "Score"])
    else:
        out_df = pd.DataFrame({
            "URL": selected["URL"] if "URL" in selected.columns else "",
            "Title": selected["Title"] if "Title" in selected.columns else "",
            "Source_Domain": selected["Source_Domain"] if "Source_Domain" in selected.columns else "",
            "Category": name,
            "Score": selected["Score"] if "Score" in selected.columns else "",
        })

    out_df.to_csv(out_path, index=False)

    logger.info(
        "Selected %s/%s winners for '%s' (fallback_used=%s) -> %s",
        stats.get("selected"),
        stats.get("winners_requested"),
        name,
        stats.get("fallback_used"),
        out_path,
    )

    return out_path



def main() -> None:

    ap = argparse.ArgumentParser(description="Select winners for a generated category YAML")
    ap.add_argument("--in", dest="in_path", required=True, help="Input queue CSV (e.g., data/Links_Queue_sorted_flags.csv)")
    ap.add_argument("--category", required=True, help="Category YAML path (name/include/exclude/winners)")
    ap.add_argument("--out", default=None, help="Optional explicit output CSV path")
    args = ap.parse_args()

    run(args.in_path, args.category, out_path=args.out)


if __name__ == "__main__":
    main()

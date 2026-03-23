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
        fallback_anchors: ["anchor1", ...]      # optional
        fallback_anchor_min_hits: <int>         # optional
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
            "AnchorHits": df.get("__anchor_hits", 0),
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
    min_qsim: float = 0.005,
    allow_exclude_only_fallback: bool = False,
    selection_summary_path: str | None = None,
    rescue_underfill: bool = False,
    rescue_max_add: int = 0,
    rescue_min_qsim: float = 0.10,
) -> Tuple[pd.DataFrame, bool, int, int]:
    name = str(cat.get("name") or "Topic").strip()
    include = [str(x).strip() for x in (cat.get("include") or []) if str(x).strip()]
    exclude = [str(x).strip() for x in (cat.get("exclude") or []) if str(x).strip()]
    fallback_anchors = [str(x).strip().lower() for x in (cat.get("fallback_anchors") or []) if str(x).strip()]
    try:
        fallback_anchor_min_hits = int(cat.get("fallback_anchor_min_hits") or 1)
    except Exception:
        fallback_anchor_min_hits = 1
    fallback_anchor_min_hits = max(1, fallback_anchor_min_hits)

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
    df["__anchor_hits"] = 0

    strict_mask = (df["__inc_hits"] > 0) & (df["__exc_hits"] == 0)
    strict_empty = not bool(strict_mask.any())

    need_qsim = strict_empty or fill_to_winners or rescue_underfill or (ranked_path is not None)
    if need_qsim:
        query = " ".join([name] + include).strip()
        df["__qsim"] = _tfidf_query_sim(query, df["__text"].tolist())

    if fallback_anchors:
        df["__anchor_hits"] = df["__text"].apply(lambda t: _count_hits(t, fallback_anchors))

    exclude_only_mask = df["__exc_hits"] == 0
    anchor_mask = df["__anchor_hits"] > 0
    anchor_gate_mask = df["__anchor_hits"] >= int(fallback_anchor_min_hits)
    qsim_base_mask = (df["__qsim"] >= float(min_qsim)) & exclude_only_mask
    qsim_mask = (qsim_base_mask & anchor_gate_mask) if fallback_anchors else qsim_base_mask

    strict_count = int(strict_mask.sum())
    anchor_count = int(anchor_mask.sum())
    anchor_gate_count = int(anchor_gate_mask.sum()) if fallback_anchors else 0
    qsim_base_count = int(qsim_base_mask.sum())
    qsim_count = int(qsim_mask.sum())
    qsim_rejected_by_anchor_count = int((qsim_base_mask & ~anchor_gate_mask).sum()) if fallback_anchors else 0
    exclude_only_count = int(exclude_only_mask.sum())
    non_excluded_below_qsim_count = int((exclude_only_mask & (df["__qsim"] < float(min_qsim))).sum())
    positive_qsim_count = int((df["__qsim"] > 0).sum())
    max_qsim = float(df["__qsim"].max()) if len(df) else 0.0

    cand = df[strict_mask].copy()
    fallback_used = False
    pool = "strict"

    if cand.empty:
        fallback_used = True
        pool = "fallback_qsim"
        cand = df[qsim_mask].copy()
        if cand.empty and bool(allow_exclude_only_fallback):
            pool = "fallback_exclude_only"
            cand = df[exclude_only_mask].copy()

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

    # Bounded rescue: ONLY when underfilled and explicitly enabled.
    # Underfill is acceptable by default; rescue must be opt-in.
    if rescue_underfill:
        underfill = cap - len(selected)
        if underfill > 0:
            target_len = int(offset) + int(cap)
            need_more = max(0, target_len - len(ranked))
            max_add = max(0, int(rescue_max_add or 0))
            logger.info(
                "Rescue underfill triggered: selected=%s/%s offset=%s ranked_total=%s need=%s max_add=%s min_qsim=%s",
                len(selected),
                cap,
                int(offset),
                len(ranked),
                need_more,
                max_add,
                float(rescue_min_qsim),
            )

            added = 0
            if need_more > 0 and max_add > 0:
                add_n = min(need_more, max_add)
                remaining = df[~df["URL"].isin(ranked["URL"])].copy()
                remaining = remaining[remaining["__exc_hits"] == 0]
                remaining = remaining[remaining["__qsim"] >= float(rescue_min_qsim)]
                remaining.sort_values(
                    by=["__qsim", "Quality4", "Quality2", "Score", "URL"],
                    ascending=[False, False, False, False, True],
                    inplace=True,
                    kind="mergesort",
                )
                fill = remaining.drop_duplicates(subset=["URL"]).head(add_n).copy()
                added = int(len(fill))

                if added > 0:
                    fallback_used = True
                    pool = f"{pool}+rescue"
                    ranked = (
                        pd.concat([ranked, fill], ignore_index=True)
                        .drop_duplicates(subset=["URL"])
                        .reset_index(drop=True)
                    )
                    ranked["__rank"] = list(range(1, len(ranked) + 1))
                    selected = ranked.iloc[offset : offset + cap].copy()

            logger.info(
                "Rescue underfill done: added=%s new_ranked_total=%s new_selected=%s/%s",
                added,
                len(ranked),
                len(selected),
                cap,
            )
        else:
            logger.info(
                "Rescue underfill not needed: selected=%s/%s offset=%s ranked_total=%s",
                len(selected),
                cap,
                int(offset),
                len(ranked),
            )


    if ranked_path:
        Path(ranked_path).parent.mkdir(parents=True, exist_ok=True)
        _format_out(ranked, topic_name=name, fallback_used=fallback_used, pool=pool).to_csv(ranked_path, index=False)

    Path(selected_path).parent.mkdir(parents=True, exist_ok=True)
    out_selected = _format_out(selected, topic_name=name, fallback_used=fallback_used, pool=pool)
    out_selected.to_csv(selected_path, index=False)

    if len(ranked) == 0:
        stop_reason = "no_candidates_passing_anchor_gate" if fallback_anchors else "no_candidates_passing_quality_gate"
    elif offset >= len(ranked):
        stop_reason = "offset_beyond_ranked_after_anchor_gate" if fallback_anchors else "offset_beyond_ranked_after_quality_gate"
    elif len(selected) < cap:
        if pool.startswith("strict"):
            stop_reason = "underfilled_after_strict_topic_gate"
        elif pool.startswith("fallback_qsim"):
            stop_reason = "underfilled_after_anchor_gate" if fallback_anchors else "underfilled_after_qsim_quality_gate"
        elif pool.startswith("fallback_exclude_only"):
            stop_reason = "underfilled_after_exclude_only_fallback"
        else:
            stop_reason = "underfilled_after_quality_gate"
    else:
        if pool == "strict":
            stop_reason = "filled_from_strict"
        elif pool == "fallback_qsim":
            stop_reason = "filled_from_qsim_anchor_fallback" if fallback_anchors else "filled_from_qsim_fallback"
        elif pool == "fallback_exclude_only":
            stop_reason = "filled_from_exclude_only_fallback"
        else:
            stop_reason = f"filled_from_{pool}"

    summary = {
        "topic": name,
        "offset": int(offset),
        "cap": int(cap),
        "min_qsim": float(min_qsim),
        "fallback_anchors": fallback_anchors,
        "fallback_anchor_min_hits": int(fallback_anchor_min_hits),
        "allow_exclude_only_fallback": bool(allow_exclude_only_fallback),
        "fallback_used": bool(fallback_used),
        "candidate_pool": pool,
        "strict_candidate_count": strict_count,
        "anchor_candidate_count": anchor_count,
        "anchor_gate_candidate_count": anchor_gate_count,
        "qsim_base_candidate_count": qsim_base_count,
        "qsim_candidate_count": qsim_count,
        "qsim_rejected_by_anchor_count": qsim_rejected_by_anchor_count,
        "exclude_only_candidate_count": exclude_only_count,
        "non_excluded_below_qsim_count": non_excluded_below_qsim_count,
        "positive_qsim_count": positive_qsim_count,
        "max_qsim": max_qsim,
        "ranked_rows": int(len(ranked)),
        "selected_rows": int(len(selected)),
        "stop_reason": stop_reason,
        "rescue_underfill": bool(rescue_underfill),
        "rescue_max_add": int(rescue_max_add or 0),
        "rescue_min_qsim": float(rescue_min_qsim),
    }

    summary_path = selection_summary_path
    if not summary_path:
        sp = Path(selected_path)
        summary_path = str(sp.with_suffix(".selection_summary.json")) if sp.suffix else str(sp.with_name(sp.name + ".selection_summary.json"))

    summary_file = Path(summary_path)
    summary_file.parent.mkdir(parents=True, exist_ok=True)

    import json
    summary_file.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    logger.info(
        "Selection summary: stop_reason=%s strict=%s anchors_any=%s anchors_gate=%s qsim_base=%s qsim=%s ranked=%s selected=%s min_qsim=%s anchor_min_hits=%s allow_exclude_only_fallback=%s -> %s",
        stop_reason,
        strict_count,
        anchor_count,
        anchor_gate_count,
        qsim_base_count,
        qsim_count,
        len(ranked),
        len(selected),
        float(min_qsim),
        int(fallback_anchor_min_hits),
        bool(allow_exclude_only_fallback),
        summary_path,
    )

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
    min_qsim: float = 0.005,
    allow_exclude_only_fallback: bool = False,
    selection_summary_path: str | None = None,
    rescue_underfill: bool = False,
    rescue_max_add: int = 0,
    rescue_min_qsim: float = 0.10,
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
        allow_exclude_only_fallback=bool(allow_exclude_only_fallback),
        selection_summary_path=selection_summary_path,
        rescue_underfill=bool(rescue_underfill),
        rescue_max_add=int(rescue_max_add or 0),
        rescue_min_qsim=float(rescue_min_qsim),
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
        default=0.005,
        help="Minimum TF-IDF similarity score for semantic fallback/fill. Default 0.005.",
    )
    ap.add_argument(
        "--allow-exclude-only-fallback",
        action="store_true",
        help="Allow a final exclude-only fallback when strict and semantic fallback produce no candidates. Default off.",
    )
    ap.add_argument(
        "--selection-summary-out",
        default=None,
        help="Write selection audit JSON. Default: alongside selected CSV as *.selection_summary.json.",
    )
    ap.add_argument(
        "--rescue-underfill",
        action="store_true",
        help=(
            "Attempt a bounded semantic rescue ONLY when the selected slice underfills the operational cap. "
            "Rescue draws from remaining non-excluded candidates with QuerySim >= --rescue-min-qsim."
        ),
    )
    ap.add_argument(
        "--rescue-max-add",
        type=int,
        default=0,
        help="Maximum additional candidates to append during rescue (bounded). 0 disables rescue fill.",
    )
    ap.add_argument(
        "--rescue-min-qsim",
        type=float,
        default=0.10,
        help="Minimum TF-IDF similarity (QuerySim) for rescue candidates. Default 0.10.",
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
        allow_exclude_only_fallback=bool(args.allow_exclude_only_fallback),
        selection_summary_path=args.selection_summary_out,
        rescue_underfill=bool(args.rescue_underfill),
        rescue_max_add=int(args.rescue_max_add or 0),
        rescue_min_qsim=float(args.rescue_min_qsim),
    )


if __name__ == "__main__":
    main()

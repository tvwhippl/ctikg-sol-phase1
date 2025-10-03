#!/usr/bin/env python3
import argparse, sys, os, re, json
import pandas as pd
import yaml

def build_out_path(cat_yaml_path: str) -> str:
    with open(cat_yaml_path, "r", encoding="utf-8") as fh:
        spec = yaml.safe_load(fh)
    name = spec.get("name", "Open_Topic")
    safe = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_")
    os.makedirs("data", exist_ok=True)
    return f"data/Selected_{safe}.csv", spec

def pick_sort(df: pd.DataFrame):
    # Prefer descending by 'score' if present; otherwise ascending by 'rank'; otherwise keep current order.
    if "score" in df.columns:
        return df.sort_values("score", ascending=False)
    if "rank" in df.columns:
        return df.sort_values("rank", ascending=True)
    return df

def select_open(df: pd.DataFrame, limit: int) -> pd.DataFrame:
    df2 = pick_sort(df)
    # Keep the columns scrape_selected.py needs; at minimum 'url'
    keep = [c for c in ["url", "URL", "title", "summary", "source_domain"] if c in df2.columns]
    if not keep:
        # We at least need url/URL
        raise SystemExit("[ERROR] input CSV has no 'url' or 'URL' column")
    # Normalize URL column name to 'url'
    if "URL" in df2.columns and "url" not in df2.columns:
        df2 = df2.rename(columns={"URL": "url"})
        keep = ["url"] + [c for c in keep if c != "URL"]
    return df2[keep].head(limit)

def select_by_keywords(df: pd.DataFrame, keywords, limit: int) -> pd.DataFrame:
    if not keywords:
        return df.head(0)
    # Build a simple contains-any matcher over title/summary/url
    hay_cols = [c for c in ["title", "summary", "url", "URL"] if c in df.columns]
    if not hay_cols:
        hay_cols = [df.columns[0]]  # worst-case fallback
    kws = [str(k).strip() for k in keywords if str(k).strip()]
    patt = re.compile("|".join(re.escape(k) for k in kws), flags=re.I)
    mask = df[hay_cols].astype(str).agg(" ".join, axis=1).str.contains(patt, na=False)
    out = df.loc[mask]
    # Normalize URL column if needed
    if "URL" in out.columns and "url" not in out.columns:
        out = out.rename(columns={"URL": "url"})
    keep = [c for c in ["url", "title", "summary", "source_domain"] if c in out.columns]
    return pick_sort(out)[keep].head(limit)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_csv", required=True)
    ap.add_argument("--category", required=True, help="Path to generated YAML")
    ap.add_argument("--limit", type=int, default=500)
    args = ap.parse_args()

    out_csv, spec = build_out_path(args.category)
    open_mode = bool(spec.get("open", False))
    keywords = spec.get("keywords") or []

    df = pd.read_csv(args.in_csv)
    if "URL" in df.columns and "url" not in df.columns:
        df = df.rename(columns={"URL": "url"})

    if open_mode or not keywords:
        print("[INFO] open-category selection: no keyword filter; taking top rows", file=sys.stderr)
        sel = select_open(df, args.limit)
    else:
        print(f"[INFO] keyword selection with {len(keywords)} keywords", file=sys.stderr)
        sel = select_by_keywords(df, keywords, args.limit)

    rows = len(sel)
    sel.to_csv(out_csv, index=False)
    print(f"[OK] wrote {rows} rows -> {out_csv}")
    if rows == 0:
        print("[WARN] 0 rows selected; downstream scrape will be a no-op", file=sys.stderr)
        return 2
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

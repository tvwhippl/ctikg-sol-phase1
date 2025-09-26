#!/usr/bin/env python3
import argparse, os, re, sys, yaml, pandas as pd, numpy as np

def sanitize(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_")

def compile_re(terms):
    if not terms: return None
    return re.compile("|".join(terms), re.IGNORECASE)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", default="data/Links_Queue_sorted_flags.csv")
    ap.add_argument("--category", required=True, help="Path to category YAML")
    ap.add_argument("--outdir", default="data")
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.category, "r", encoding="utf-8"))
    name = cfg["name"]; winners = int(cfg.get("winners", 100))
    inc = compile_re(cfg.get("include", []))
    exc = compile_re(cfg.get("exclude", []))

    df = pd.read_csv(args.inp)
    cols = [c for c in df.columns if c.lower() in
            {"title","snippet","summary","description","url","source_domain","domain"}]
    text = (df[cols].astype(str).agg(" ".join, axis=1) if cols
            else df.astype(str).agg(" ".join, axis=1))

    hits = pd.Series([True]*len(df))
    if inc is not None: hits &= text.str.contains(inc, na=False)
    if exc is not None: hits &= ~text.str.contains(exc, na=False)

    keep = df.loc[hits].copy()
    if keep.empty:
        print(f"[WARN] No matches for category: {name}")
        sys.exit(0)

    score_col = next((c for c in keep.columns if c.lower()=="score"), None)
    if not score_col:
        # fallback to recency if available
        keep["__score"] = pd.to_datetime(keep.get("Publish_Date"), errors="coerce").astype("int64").fillna(0)
        score_col = "__score"

    url_col = next((c for c in keep.columns if c.lower()=="url"), None)
    if url_col: keep = keep.drop_duplicates(subset=[url_col], keep="first")

    keep["Category"] = name; keep["Selected"] = True
    ordered = [c for c in ["URL","url","Category","Selected",score_col] if c in keep.columns]
    ordered += [c for c in keep.columns if c not in ordered]
    keep = keep[ordered].sort_values(score_col, ascending=False).head(winners)

    os.makedirs(args.outdir, exist_ok=True)
    out = os.path.join(args.outdir, f"Selected_{sanitize(name)}.csv")
    keep.to_csv(out, index=False)
    print(f"[DONE] {len(keep)} winners → {out}")

if __name__ == "__main__":
    main()

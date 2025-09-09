#!/usr/bin/env python3
"""
select_winners.py
-----------------
Auto-selects "winners" per category from a queue CSV (with flags) and writes:
- Updated queue CSV with Status=Selected for winners
- Per-category Selected CSVs
- Selected_master.csv (all winners)
- Selected_summary.csv (counts per category)

Assumes your CSV has these columns (created by make_helper_flags.py):
URL, Title, Source_Domain, Category_Guess, Score, RepFlag, SigFlag, Quality2, Quality4, Status (optional)

Default policy (tunable with args):
- Target N per category (default 100)
- Prefer rows with Quality4 >= 2 (reputable + has CVE/MITRE/IOC signals)
- Rank by: Quality4 desc, Quality2 desc, RepFlag desc, SigFlag desc, Score desc
- Skip rows already marked Status=Rejected (unless --include_rejected)

Examples:
  python3 select_winners.py --in Links_Queue_sorted_flags.csv --out Links_Queue_with_selected.csv
  python3 select_winners.py --in Links_Queue_sorted_flags.csv --per_category 120
  python3 select_winners.py --in Links_Queue_sorted_flags.csv --min_quality4 3
  python3 select_winners.py --in Links_Queue_sorted_flags.csv --reset_selected
  python3 select_winners.py --in Links_Queue_sorted_flags.csv \
     --categories "SSH & Credential Abuse" "Cryptomining on HPC" "NFS / File-Share Exposure"
"""

import argparse, os, re, sys
import pandas as pd

DEFAULT_CATEGORIES = [
    "SSH & Credential Abuse",
    "Cryptomining on HPC",
    "NFS / File-Share Exposure",
    "JupyterHub / Open OnDemand",
]

def safe_name(s: str) -> str:
    return re.sub(r'[^A-Za-z0-9]+', '_', s).strip('_')

def ensure_cols(df, cols_defaults):
    for c, default in cols_defaults.items():
        if c not in df.columns:
            df[c] = default
    return df

def to_numeric(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df

def select_for_category(df_cat, n_target, min_quality4, include_rejected=False, status_col="Status"):
    # Exclude rejected unless explicitly included
    if not include_rejected and status_col in df_cat.columns:
        df_cat = df_cat[df_cat[status_col].str.lower().ne("rejected")]

    # Priority pool: Quality4 >= threshold
    strong = df_cat[df_cat["Quality4"] >= min_quality4].copy()
    weak   = df_cat[df_cat["Quality4"] <  min_quality4].copy()

    key = ["Quality4","Quality2","RepFlag","SigFlag","Score"]
    strong = strong.sort_values(key, ascending=[False]*len(key))
    weak   = weak.sort_values(key,   ascending=[False]*len(key))

    selected = strong.head(n_target)
    if len(selected) < n_target:
        need = n_target - len(selected)
        selected = pd.concat([selected, weak.head(need)], ignore_index=True)

    # Dedup by URL just in case
    return selected.drop_duplicates(subset=["URL"], keep="first").head(n_target)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True, help="Input CSV (with RepFlag/SigFlag/Quality2/Quality4)")
    ap.add_argument("--out", dest="out_path", default=None, help="Output CSV (queue with Status updated)")
    ap.add_argument("--categories", nargs="*", default=None, help="Categories to select (defaults to 4 core categories)")
    ap.add_argument("--per_category", type=int, default=100, help="Winners per category (default 100)")
    ap.add_argument("--min_quality4", type=int, default=2, help="Minimum Quality4 for priority selection (default 2)")
    ap.add_argument("--include_rejected", action="store_true", help="Allow selecting items marked Rejected")
    ap.add_argument("--reset_selected", action="store_true", help="Clear existing Status=Selected before selecting")
    ap.add_argument("--status_field", default="Status", help="Column name for selection status (default 'Status')")
    args = ap.parse_args()

    if not os.path.exists(args.in_path):
        print(f"[ERROR] Input not found: {args.in_path}", file=sys.stderr); sys.exit(1)

    df = pd.read_csv(args.in_path)

    # Ensure required columns
    df = ensure_cols(df, {
        "Category_Guess":"", "Score":0, "RepFlag":0, "SigFlag":0, "Quality2":0, "Quality4":0,
        args.status_field: ""
    })
    to_numeric(df, ["Score","RepFlag","SigFlag","Quality2","Quality4"])
    if "URL" not in df.columns:
        print("[ERROR] CSV missing 'URL' column.", file=sys.stderr); sys.exit(2)

    categories = args.categories or DEFAULT_CATEGORIES
    present = set(str(x) for x in df["Category_Guess"].dropna().unique())
    missing = [c for c in categories if c not in present]
    if missing:
        print(f"[WARN] Categories not found in CSV: {missing}")

    # Optionally clear existing Selected (never clear Rejected)
    if args.reset_selected and args.status_field in df.columns:
        mask = df[args.status_field].str.lower().eq("selected")
        df.loc[mask, args.status_field] = ""

    # Build selections
    all_selected = []
    for cat in categories:
        df_cat = df[df["Category_Guess"] == cat].copy()
        if df_cat.empty:
            print(f"[INFO] No rows for category '{cat}'")
            continue
        chosen = select_for_category(
            df_cat, args.per_category, args.min_quality4,
            include_rejected=args.include_rejected, status_col=args.status_field
        )
        chosen["__SelectedCategory"] = cat
        all_selected.append(chosen)

    if not all_selected:
        print("[WARN] No selections made."); sys.exit(0)

    winners = pd.concat(all_selected, ignore_index=True).drop_duplicates(subset=["URL"], keep="first")

    # Update Status in main df (do not overwrite Rejected)
    status_col = args.status_field
    if status_col not in df.columns:
        df[status_col] = ""
    rejected_mask = df[status_col].str.lower().eq("rejected") if status_col in df.columns else pd.Series(False, index=df.index)
    sel_mask = df["URL"].isin(winners["URL"]) & ~rejected_mask
    df.loc[sel_mask, status_col] = "Selected"

    # Write outputs (same folder as input)
    base_dir = os.path.dirname(os.path.abspath(args.in_path))
    out_path = args.out_path or os.path.join(base_dir, "Links_Queue_with_selected.csv")
    df.to_csv(out_path, index=False)

    master_path = os.path.join(base_dir, "Selected_master.csv")
    winners.to_csv(master_path, index=False)

    per_cat_paths = []
    for cat in winners["__SelectedCategory"].unique():
        sub = winners[winners["__SelectedCategory"] == cat]
        p = os.path.join(base_dir, f"Selected_{safe_name(cat)}.csv")
        sub.to_csv(p, index=False)
        per_cat_paths.append(p)

    summary = winners["__SelectedCategory"].value_counts().rename_axis("Category").reset_index(name="SelectedCount")
    summary_path = os.path.join(base_dir, "Selected_summary.csv")
    summary.to_csv(summary_path, index=False)

    print(f"[DONE] Updated queue: {out_path}")
    print(f"[DONE] Master winners: {master_path}")
    print(f"[DONE] Per-category files: {per_cat_paths}")
    print(f"[DONE] Summary: {summary_path}")

if __name__ == "__main__":
    main()

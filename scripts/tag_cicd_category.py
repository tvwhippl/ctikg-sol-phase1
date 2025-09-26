#!/usr/bin/env python3
import argparse, pandas as pd, re

TERMS = [
  r"ci/cd", r"\bci-?cd\b", r"\bcicd\b",
  r"github actions", r"actions runner", r"self-hosted runner", r"actions cache",
  r"gitlab ci", r"gitlab runner", r"\bjenkins\b", r"jenkins pipeline",
  r"azure devops", r"azure pipelines", r"\bado\b pipelines?",
  r"workflow injection", r"malicious workflow", r"artifact poisoning", r"cache poisoning",
  r"\boidc\b", r"oidc federation", r"github oidc",
  r"secrets exposure", r"build pipeline", r"supply chain"
]
PAT = re.compile("|".join(TERMS), re.IGNORECASE)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", default="data/Links_Queue_sorted_flags.csv")
    ap.add_argument("--out", dest="out", default="data/Links_Queue_sorted_flags_ci.csv")
    ap.add_argument("--category_name", default="CI/CD Pipeline Attacks")
    args = ap.parse_args()

    df = pd.read_csv(args.inp)
    cols = [c for c in df.columns if c.lower() in {"title","snippet","summary","description","url","domain"}]
    text = (df[cols].astype(str).agg(" ".join, axis=1) if cols else df.astype(str).agg(" ".join, axis=1))
    hits = text.str.contains(PAT)
    catcol = next((c for c in df.columns if c.lower().startswith("category")), None)
    if not catcol:
        df["Category"] = ""
        catcol = "Category"
    df.loc[hits, catcol] = args.category_name
    df.to_csv(args.out, index=False)
    print(f"Tagged {hits.sum()} rows as {args.category_name}; wrote {args.out}")

if __name__ == "__main__":
    main()

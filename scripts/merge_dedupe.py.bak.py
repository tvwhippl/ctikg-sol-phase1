#!/usr/bin/env python3
import sys, pandas as pd
if len(sys.argv) < 3:
    print("Usage: merge_dedupe.py out.csv in1.csv in2.csv ..."); sys.exit(1)
out = sys.argv[1]
dfs = [pd.read_csv(p) for p in sys.argv[2:]]
df = pd.concat(dfs, ignore_index=True)
df = df.drop_duplicates(subset=["URL"]).reset_index(drop=True)
df.to_csv(out, index=False)
print("Wrote", len(df), "rows to", out)

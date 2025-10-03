#!/usr/bin/env python3
import os, sys
import pandas as pd

assert os.path.isfile("results/scraped_corpus.jsonl"), "missing scraped_corpus.jsonl"
assert os.path.isfile("exports/ctikg_input.csv"), "missing exports/ctikg_input.csv"

df = pd.read_csv("exports/ctikg_input.csv")
assert "sentence" in df.columns, "missing `sentence` column"
assert (df["sentence"].astype(str).str.strip() != "").all(), "empty sentences exist"

assert len(df) > 0, "export is empty (0 rows)"
print("OK: verification passed. rows:", len(df))

#!/usr/bin/env python3

"""Verify that an open-topic export is sane.

Historically this script hard-coded:
  - results/scraped_corpus.jsonl
  - exports/ctikg_input.csv

For multi-topic / batch runs we also support explicit paths.
"""

from __future__ import annotations

import argparse
import os

import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--corpus", default="results/scraped_corpus.jsonl", help="Path to scraped_corpus.jsonl")
    ap.add_argument("--csv", default="exports/ctikg_input.csv", help="Path to ctikg_input.csv")
    ap.add_argument("--min-rows", type=int, default=1, help="Minimum required rows in ctikg_input.csv")
    args = ap.parse_args()

    assert os.path.isfile(args.corpus), f"missing corpus: {args.corpus}"
    assert os.path.isfile(args.csv), f"missing export csv: {args.csv}"

    df = pd.read_csv(args.csv)
    assert "sentence" in df.columns, "missing `sentence` column"
    assert (df["sentence"].astype(str).str.strip() != "").all(), "empty sentences exist"

    assert len(df) >= int(args.min_rows), f"export is too small ({len(df)} rows < {args.min_rows})"
    print("OK: verification passed. rows:", len(df))


if __name__ == "__main__":
    main()

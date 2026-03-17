#!/usr/bin/env python3
import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


def normalize_text(rec: dict) -> str:
    raw = str(rec.get("text") or rec.get("content") or "").strip()
    return " ".join(raw.split()).strip()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True, help="Run directory containing scrape/scraped_corpus.jsonl")
    ap.add_argument("--out-dir", default=None, help="Output directory (default: <run-dir>/llm4cti)")
    ap.add_argument("--min-chars", type=int, default=200, help="Minimum normalized content length")
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    in_path = run_dir / "scrape" / "scraped_corpus.jsonl"
    out_dir = Path(args.out_dir) if args.out_dir else (run_dir / "llm4cti")
    out_dir.mkdir(parents=True, exist_ok=True)

    if not in_path.exists():
        raise SystemExit(f"Missing input: {in_path}")

    rows = []
    seen_urls = set()

    with in_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)

            text = normalize_text(rec)
            url = str(rec.get("url") or "").strip()
            if not text or len(text) < args.min_chars:
                continue
            if url and url in seen_urls:
                continue
            if url:
                seen_urls.add(url)

            rows.append({
                "ArticleIndex": len(rows) + 1,
                "title": str(rec.get("title") or "").strip(),
                "url": url,
                "source_domain": str(rec.get("source_domain") or "").strip(),
                "category": str(rec.get("category") or "").strip(),
                "content": text,
                "content_chars": len(text),
                "text_sha256": hashlib.sha256(text.encode("utf-8", "ignore")).hexdigest(),
            })

    df = pd.DataFrame(rows)
    csv_path = out_dir / "llm4cti_articles.csv"
    xlsx_path = out_dir / "Articles.xlsx"
    meta_path = out_dir / "llm4cti_articles_meta.json"

    df.to_csv(csv_path, index=False)
    df.to_excel(xlsx_path, index=False)
    meta_path.write_text(json.dumps({
        "docs": len(df),
        "columns": df.columns.tolist(),
        "sources": df["source_domain"].value_counts().to_dict() if len(df) else {},
        "categories": df["category"].value_counts().to_dict() if len(df) else {},
        "input_jsonl": str(in_path),
    }, indent=2), encoding="utf-8")

    print("docs:", len(df))
    print("csv:", csv_path)
    print("xlsx:", xlsx_path)
    print("meta:", meta_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

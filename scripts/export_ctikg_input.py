#!/usr/bin/env python3
"""
Robust exporter for Phase-1 (open-topic).
- Reads results/scraped_corpus.jsonl (required)
- Writes exports/ctikg_input.csv with a `sentence` column
- Does NOT require chunks; falls back to reading text from either:
  (1) JSONL's "text" field, or
  (2) file at "txt_path" if present
"""
import argparse, csv, json, os, re, sys
from pathlib import Path

def sent_split(text: str, hard_len=600):
    # light sentence splitter with hard length fallback
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return []
    # try sentence-ish splits first
    sents = re.split(r"(?<=[.!?])\s+(?=[A-Z0-9])", text)
    out = []
    for s in sents:
        s = s.strip()
        if not s:
            continue
        # hard cap long runs ~600 chars with mild overlap
        while len(s) > hard_len:
            cut = s.rfind(" ", 0, hard_len)
            if cut < hard_len // 2:  # no good cut; just slice
                cut = hard_len
            out.append(s[:cut].strip())
            s = s[cut:].lstrip()
        if s:
            out.append(s)
    return out

def read_text_from_row(row: dict) -> str:
    # Priority 1: direct "text" in JSONL
    t = row.get("text") or row.get("content")
    if t:
        return t
    # Priority 2: artifact text path
    p = row.get("txt_path")
    if p and os.path.isfile(p):
        try:
            with open(p, "r", encoding="utf-8", errors="ignore") as fh:
                return fh.read()
        except Exception:
            return ""
    return ""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_jsonl", required=True)
    ap.add_argument("--out_csv", required=True)
    ap.add_argument("--out_docs", required=True)
    args = ap.parse_args()

    in_path  = Path(args.in_jsonl)
    out_csv  = Path(args.out_csv)
    out_docs = Path(args.out_docs)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_docs.parent.mkdir(parents=True, exist_ok=True)

    rows_written = 0
    docs_meta = {"docs": 0, "rows": 0, "sources": {}}

    with open(out_csv, "w", encoding="utf-8", newline="") as fout:
        w = csv.DictWriter(fout, fieldnames=["sentence","url","category","title","source_domain"])
        w.writeheader()

        if not in_path.is_file():
            print(f"[WARN] no input jsonl at {in_path}", file=sys.stderr)
        else:
            with open(in_path, "r", encoding="utf-8") as fh:
                for line in fh:
                    try:
                        rec = json.loads(line)
                    except Exception:
                        continue
                    text = read_text_from_row(rec)
                    if not text:
                        continue
                    sents = sent_split(text)
                    if not sents:
                        continue
                    url  = rec.get("url") or rec.get("URL") or ""
                    cat  = rec.get("category") or ""
                    tit  = rec.get("title") or ""
                    dom  = rec.get("source_domain") or rec.get("domain") or ""
                    docs_meta["docs"] += 1
                    docs_meta["sources"][dom] = docs_meta["sources"].get(dom, 0) + 1
                    for s in sents:
                        w.writerow({"sentence": s, "url": url, "category": cat, "title": tit, "source_domain": dom})
                        rows_written += 1

    docs_meta["rows"] = rows_written
    with open(out_docs, "w", encoding="utf-8") as md:
        json.dump(docs_meta, md, ensure_ascii=False, indent=2)

    print(f"[OK] wrote {rows_written} rows -> {out_csv} and docs meta -> {out_docs}")

if __name__ == "__main__":
    raise SystemExit(main())

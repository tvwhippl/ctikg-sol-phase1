#!/usr/bin/env python3
"""
Phase-1 exporter (open-topic):
- Writes exports/ctikg_input.csv with a `sentence` column
- Primary: read text from results/scraped_corpus.jsonl (keys: text/content/txt_path)
- Fallback: if 0 rows, read results/scrape_log.csv (keys: txt_path,status,URL,category,title,source_domain)
"""
import argparse, csv, json, os, re, sys
from pathlib import Path

def sent_split(text: str, hard_len=600):
    text = re.sub(r"\s+", " ", text or "").strip()
    if not text:
        return []
    parts = re.split(r"(?<=[.!?])\s+(?=[A-Z0-9])", text)
    out = []
    for s in parts:
        s = s.strip()
        if not s:
            continue
        while len(s) > hard_len:
            cut = s.rfind(" ", 0, hard_len)
            if cut < hard_len // 2:
                cut = hard_len
            out.append(s[:cut].strip())
            s = s[cut:].lstrip()
        if s:
            out.append(s)
    return out

def read_text_from_row(rec: dict) -> str:
    t = rec.get("text") or rec.get("content")
    if t:
        return t
    p = rec.get("txt_path")
    if p and os.path.isfile(p):
        try:
            with open(p, "r", encoding="utf-8", errors="ignore") as fh:
                return fh.read()
        except Exception:
            return ""
    return ""

def write_rows(rows_iter, writer, docs_meta):
    rows_written = 0
    for payload in rows_iter:
        text = payload["text"]
        url  = payload.get("url","")
        cat  = payload.get("category","")
        tit  = payload.get("title","")
        dom  = payload.get("source_domain","")
        sents = sent_split(text)
        if not sents:
            continue
        docs_meta["docs"] += 1
        docs_meta["sources"][dom] = docs_meta["sources"].get(dom, 0) + 1
        for s in sents:
            writer.writerow({"sentence": s, "url": url, "category": cat, "title": tit, "source_domain": dom})
            rows_written += 1
    return rows_written

def from_jsonl(in_path: Path):
    if not in_path.is_file():
        return
    with open(in_path, "r", encoding="utf-8") as fh:
        for line in fh:
            try:
                rec = json.loads(line)
            except Exception:
                continue
            text = read_text_from_row(rec)
            if not text:
                continue
            yield {
                "text": text,
                "url": rec.get("url") or rec.get("URL") or "",
                "category": rec.get("category",""),
                "title": rec.get("title",""),
                "source_domain": rec.get("source_domain") or rec.get("domain") or "",
            }

def from_log_csv(log_path: Path):
    if not log_path.is_file():
        return
    with open(log_path, "r", encoding="utf-8", newline="") as cf:
        rdr = csv.DictReader(cf)
        for rec in rdr:
            status = (rec.get("status","") or "").lower()
            if status not in ("ok","200","success",""):
                continue
            p = rec.get("txt_path") or ""
            if not p or not os.path.isfile(p):
                continue
            try:
                with open(p, "r", encoding="utf-8", errors="ignore") as fh:
                    text = fh.read()
            except Exception:
                continue
            yield {
                "text": text,
                "url": rec.get("URL") or rec.get("url") or "",
                "category": rec.get("category",""),
                "title": rec.get("title",""),
                "source_domain": rec.get("source_domain",""),
            }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_jsonl", required=True)
    ap.add_argument("--out_csv", required=True)
    ap.add_argument("--out_docs", required=True)
    ap.add_argument("--log_csv", default="results/scrape_log.csv")
    args = ap.parse_args()

    in_path  = Path(args.in_jsonl)
    out_csv  = Path(args.out_csv)
    out_docs = Path(args.out_docs)
    log_path = Path(args.log_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_docs.parent.mkdir(parents=True, exist_ok=True)

    docs_meta = {"docs": 0, "rows": 0, "sources": {}}

    with open(out_csv, "w", encoding="utf-8", newline="") as fout:
        w = csv.DictWriter(fout, fieldnames=["sentence","url","category","title","source_domain"])
        w.writeheader()
        rows_written = write_rows(from_jsonl(in_path), w, docs_meta)
        if rows_written == 0:
            rows_written = write_rows(from_log_csv(log_path), w, docs_meta)

    docs_meta["rows"] = rows_written
    with open(out_docs, "w", encoding="utf-8") as md:
        json.dump(docs_meta, md, ensure_ascii=False, indent=2)

    print(f"[OK] wrote {rows_written} rows -> {out_csv} and docs meta -> {out_docs}")

if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
# Minimal chunker: optional step; no-op if content/text is empty.

import argparse, os, json, pathlib, re, sys

def iter_texts(indir: str):
    for root, _, files in os.walk(indir):
        for f in files:
            if f.lower().endswith((".txt", ".md", ".html")):
                p = os.path.join(root, f)
                try:
                    with open(p, "r", encoding="utf-8", errors="ignore") as fh:
                        yield p, fh.read()
                except Exception as e:
                    print(f"[WARN] couldn't read {p}: {e}", file=sys.stderr)

def chunk_text(t: str, target: int = 600, overlap: int = 50):
    t = re.sub(r"\s+", " ", t).strip()
    if not t:
        return []
    out, n, i = [], len(t), 0
    while i < n:
        j = min(i + target, n)
        # try to end on punctuation if reasonably close
        k = max(t.rfind(".", i, j), t.rfind("!", i, j), t.rfind("?", i, j))
        if k != -1 and k > i + target // 3:
            j = k + 1
        out.append(t[i:j].strip())
        i = max(j - overlap, j)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--category", default="unknown")
    ap.add_argument("--indir", required=True)
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
# sanitize filename so slashes/spaces don't create subfolders
safe = re.sub(r"[^A-Za-z0-9]+", "_", args.category).strip("_")
outpath = pathlib.Path(args.outdir) / f"{safe}.jsonl"

    if not os.path.isdir(args.indir):
        print(f"[info] no indir '{args.indir}' – skipping chunking.")
        outpath.touch()
        return 0

    total = 0
    with open(outpath, "w", encoding="utf-8") as out:
        for doc_id, text in iter_texts(args.indir):
            for k, ch in enumerate(chunk_text(text)):
                out.write(json.dumps({"category": args.category, "doc_id": doc_id, "chunk_id": k, "text": ch}, ensure_ascii=False) + "\n")
                total += 1
    print(f"[done] wrote {total} chunks → {outpath}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

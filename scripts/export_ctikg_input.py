import os, json, argparse, pandas as pd, re, pathlib

def sent_split(text):
    # simple sentence split; CTIKG will re-chunk anyway
    return [s.strip() for s in re.split(r'(?<=[.!?])\s+', text) if s.strip()]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_jsonl", required=True)
    ap.add_argument("--out_csv",  required=True)
    ap.add_argument("--out_docs", required=True)
    args = ap.parse_args()

    base = pathlib.Path(".")
    meta = []
    rows = []
    with open(args.in_jsonl,"r",encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            if rec.get("status")!="ok": continue
            p = rec.get("txt_path","")
            if not p or not os.path.exists(p): continue
            try:
                txt = open(p,encoding="utf-8",errors="ignore").read()
            except:
                continue
            sents = sent_split(txt)
            if not sents: continue
            # doc-level meta
            m = {
                "url": rec.get("URL") or rec.get("url"),
                "title": rec.get("title",""),
                "category": rec.get("category",""),
                "source_domain": rec.get("source_domain",""),
                "txt_path": p,
                "sentences": len(sents)
            }
            meta.append(m)
            # row-per-sentence
            for s in sents:
                rows.append({
                    "sentence": s,
                    "category": m["category"],
                    "url": m["url"],
                    "source_domain": m["source_domain"],
                    "title": m["title"]
                })
    pd.DataFrame(rows).to_csv(args.out_csv, index=False)
    with open(args.out_docs,"w",encoding="utf-8") as w:
        for m in meta:
            w.write(json.dumps(m, ensure_ascii=False)+"\n")
    print("[OK] Sentences:", len(rows))
    print("[OK] Docs meta:", len(meta))
    print("[OK] Wrote:", args.out_csv, "and", args.out_docs)

if __name__=="__main__":
    main()

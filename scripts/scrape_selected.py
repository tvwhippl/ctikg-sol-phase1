
#!/usr/bin/env python3
import argparse, csv, json, os, sys, time, re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse
import requests, trafilatura, chardet
from io import BytesIO
from pdfminer.high_level import extract_text as pdf_extract
import justext

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"

def normkey(k): 
    return (k or "").strip().lower().replace(" ", "_")

def find_url_field(fieldnames):
    # prioritize common names, but allow any col that *looks* like a URL
    priority = ["url","link","article_url","page","href"]
    keys = [normkey(k) for k in fieldnames]
    for p in priority:
        if p in keys: 
            return keys.index(p)
    # fallback: first column whose sample value looks like http(s)
    return None

def load_rows(csv_path):
    rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        idx = find_url_field(header)
        # If we couldn't find a clear URL field, scan row values
        if idx is None:
            header = [h if h else f"col{i}" for i,h in enumerate(header)]
            for r in [row for row in reader if any(row)]:
                url_idx = next((i for i,v in enumerate(r) if isinstance(v,str) and v.startswith("http")), None)
                if url_idx is None: 
                    continue
                d = dict(zip([normkey(x) for x in header], r))
                url = r[url_idx].strip()
                category = d.get("category") or d.get("topic") or "unspecified"
                title = d.get("title") or d.get("headline") or ""
                sd = d.get("source_domain") or urlparse(url).netloc
                rows.append({"url": url, "category": category, "title": title, "source_domain": sd})
            return rows
        # normal path (we know the URL column)
        header_norm = [normkey(h) for h in header]
        for r in reader:
            if not r or len(r) != len(header):
                continue
            d = dict(zip(header_norm, r))
            url = r[idx].strip()
            if not (isinstance(url,str) and url.startswith("http")):
                continue
            category = d.get("category") or d.get("topic") or "unspecified"
            title = d.get("title") or d.get("headline") or ""
            sd = d.get("source_domain") or urlparse(url).netloc
            rows.append({"url": url, "category": category, "title": title, "source_domain": sd})
    return rows

def sanitize_filename(s):
    return "".join(c if c.isalnum() or c in "-._" else "_" for c in s)[:180]

def fetch_raw(url, timeout=25):
    try:
        r = requests.get(url, headers={"User-Agent": UA, "Accept":"text/html,application/pdf;q=0.9,*/*;q=0.8"}, timeout=timeout)
        ct = r.headers.get("Content-Type","").lower()
        return r.status_code, ct, r.content
    except Exception as e:
        return 0, "", None

def html_to_text(html_bytes):
    # try charset detection
    enc = chardet.detect(html_bytes).get("encoding") or "utf-8"
    html = html_bytes.decode(enc, errors="ignore")
    # 1) Trafilatura (high recall)
    text = trafilatura.extract(html, favor_recall=True, include_comments=False, include_tables=False)
    if text and text.strip():
        return text
    # 2) jusText fallback
    try:
        paras = justext.justext(html, justext.get_stoplist("English"))
        good = [p.text for p in paras if not p.is_boilerplate]
        return "\n".join(good).strip()
    except Exception:
        return ""

def scrape_one(row, artifacts_dir, timeout=25):
    url = row["url"]
    domain = row["source_domain"]
    status, ctype, body = fetch_raw(url, timeout=timeout)
    if status != 200 or body is None:
        return {"url": url, "status": "fetch_fail", "reason": f"http_{status}", **row}
    # artifact path
    ext = ".pdf" if ("pdf" in ctype or url.lower().endswith(".pdf") or body[:4] == b"%PDF") else ".html"
    art = Path(artifacts_dir) / f"{sanitize_filename(domain)}__{sanitize_filename(urlparse(url).path or 'index')}{ext}"
    try:
        art.write_bytes(body)
    except Exception:
        pass
    # convert to text
    try:
        if ext == ".pdf":
            text = pdf_extract(BytesIO(body)) or ""
        else:
            text = html_to_text(body)
    except Exception as e:
        return {"url": url, "status": "error", "reason": f"extract_exc:{e.__class__.__name__}", "artifact": str(art), **row}
    if not text.strip():
        return {"url": url, "status": "extract_fail", "reason": "empty_text", "artifact": str(art), **row}
    return {"url": url, "status": "ok", "reason": "", "text": text, "artifact": str(art), **row}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="out_csv", required=True)  # scrape log
    ap.add_argument("--jsonl", dest="out_jsonl", required=True)
    ap.add_argument("--artifacts", dest="artifacts", required=True)
    ap.add_argument("--max_per_category", type=int, default=25)
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--ignore_robots", action="store_true",
                help="compat flag for Makefile; scraper does direct HTTP and does not consult robots.txt")
    ap.add_argument("--throttle_sec", type=float, default=0.0,
                help="sleep this many seconds between completed fetches (politeness throttle)")
    args = ap.parse_args()

    throttle = max(0.0, float(getattr(args, "throttle_sec", 0.0)))

    Path(args.artifacts).mkdir(parents=True, exist_ok=True)
    Path(os.path.dirname(args.out_csv) or ".").mkdir(parents=True, exist_ok=True)
    Path(os.path.dirname(args.out_jsonl) or ".").mkdir(parents=True, exist_ok=True)

    rows = load_rows(args.inp)
    if not rows:
        print(f"[WARN] No rows found in {args.inp}", file=sys.stderr)
        open(args.out_csv, "w").write("")
        open(args.out_jsonl, "w").write("")
        sys.exit(0)

    # cap per category
    per_cat, selected = {}, []
    for r in rows:
        c = r["category"]
        n = per_cat.get(c, 0)
        if n < args.max_per_category:
            per_cat[c] = n + 1
            selected.append(r)

    results = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futs = {ex.submit(scrape_one, r, args.artifacts): r for r in selected}
        for fut in as_completed(futs):
            res = fut.result()
            results.append(res)
            if throttle > 0:
                time.sleep(throttle)

    # logs
    log_fields = ["url","status","reason","category","source_domain","title","artifact"]
    with open(args.out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=log_fields)
        w.writeheader()
        for r in results:
            w.writerow({k: r.get(k,"") for k in log_fields})

    # corpus
    ok = [r for r in results if r.get("status") == "ok" and r.get("text")]
    with open(args.out_jsonl, "w", encoding="utf-8") as f:
        for r in ok:
            f.write(json.dumps({
                "url": r["url"],
                "title": r.get("title",""),
                "text": r["text"],
                "category": r["category"],
                "source_domain": r["source_domain"]
            }) + "\n")

    print(f"[OK] scraped={len(ok)} total={len(results)} secs={int(time.time()-t0)}")
    if not ok:
        sys.exit(2)

if __name__ == "__main__":
    main()

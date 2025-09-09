#!/usr/bin/env python3
"""
Scrape winners (Status=Selected) from the queue and produce:
- artifacts/html/<id>.html   (raw HTML when applicable)
- artifacts/pdf/<id>.pdf     (raw PDF when applicable)
- artifacts/txt/<id>.txt     (cleaned text)
- results/scraped_corpus.jsonl  (one JSON record per doc)
- results/scrape_log.csv        (status/reason per URL)

Usage:
  python3 scripts/scrape_selected.py \
    --in data/Links_Queue_with_selected.csv \
    --out results/scrape_log.csv \
    --jsonl results/scraped_corpus.jsonl \
    --artifacts artifacts \
    --max_per_category 120 \
    --concurrency 4
"""

import argparse, csv, hashlib, io, json, os, re, time, urllib.parse, warnings
from datetime import datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter, Retry
try:
    import requests_cache
except Exception:
    requests_cache = None

from bs4 import BeautifulSoup
try:
    import trafilatura
except Exception:
    trafilatura = None

try:
    from pdfminer.high_level import extract_text as pdf_extract_text
except Exception:
    pdf_extract_text = None

import pandas as pd
from urllib import robotparser
from tqdm import tqdm

UA = os.environ.get("SCRAPER_USER_AGENT", "ctikg-sol-phase1/0.1 (+https://github.com)")

def mk_dirs(base):
    base = Path(base)
    (base / "html").mkdir(parents=True, exist_ok=True)
    (base / "pdf").mkdir(parents=True, exist_ok=True)
    (base / "txt").mkdir(parents=True, exist_ok=True)
    return base

def normalized_domain(url):
    try:
        return urllib.parse.urlparse(url).netloc.lower()
    except Exception:
        return ""

def sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256(); h.update(b); return h.hexdigest()

def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()

def allowed_by_robots(rob_cache, url, ua) -> bool:
    dom = normalized_domain(url)
    if dom not in rob_cache:
        rp = robotparser.RobotFileParser()
        scheme = urllib.parse.urlparse(url).scheme or "https"
        robots_url = f"{scheme}://{dom}/robots.txt"
        try:
            rp.set_url(robots_url); rp.read()
        except Exception:
            # assume allowed if robots not reachable
            rob_cache[dom] = None
            return True
        rob_cache[dom] = rp
    rp = rob_cache[dom]
    return True if rp is None else rp.can_fetch(ua, url)

def build_session(cache=True):
    if cache and requests_cache is not None:
        requests_cache.install_cache("scraper_cache", expire_after=60*60*24*7)
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=0.7, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": UA, "Accept": "*/*"})
    return s

def is_pdf_response(resp, url):
    ctype = resp.headers.get("Content-Type","").lower()
    if "application/pdf" in ctype: return True
    if re.search(r"\.pdf($|\?)", url.lower()): return True
    return False

def clean_html_to_text(html, base_url=None):
    txt = ""
    if trafilatura is not None:
        try:
            txt = trafilatura.extract(html, include_comments=False, include_tables=False, url=base_url) or ""
        except Exception:
            txt = ""
    if not txt:
        try:
            soup = BeautifulSoup(html, "lxml")
            for s in soup(["script","style","noscript"]): s.extract()
            txt = soup.get_text(separator="\n")
            # lightweight de-dup of empty lines
            lines = [ln.strip() for ln in txt.splitlines()]
            txt = "\n".join([ln for ln in lines if ln])
        except Exception:
            txt = ""
    return txt

def pdf_bytes_to_text(b):
    if pdf_extract_text is None:
        return ""
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return pdf_extract_text(io.BytesIO(b)) or ""
    except Exception:
        return ""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", dest="log_csv", default="results/scrape_log.csv")
    ap.add_argument("--jsonl", dest="jsonl_path", default="results/scraped_corpus.jsonl")
    ap.add_argument("--artifacts", default="artifacts")
    ap.add_argument("--max_per_category", type=int, default=120)
    ap.add_argument("--concurrency", type=int, default=4)  # reserved for future; current is sequential + polite throttle
    ap.add_argument("--ignore_robots", action="store_true")
    ap.add_argument("--throttle_sec", type=float, default=0.8)
    args = ap.parse_args()

    artifacts = mk_dirs(args.artifacts)
    Path("results").mkdir(exist_ok=True)

    df = pd.read_csv(args.in_path)
    if "Status" in df.columns:
        df = df[df["Status"].astype(str).str.lower() == "selected"]
    if "Category_Guess" not in df.columns:
        df["Category_Guess"] = ""
    if "Publish_Date" not in df.columns:
        df["Publish_Date"] = ""

    # cap per category
    keep = []
    for cat, grp in df.groupby("Category_Guess"):
        grp = grp.copy()
        if "Score" in grp.columns:
            grp = grp.sort_values("Score", ascending=False)
        keep.append(grp.head(args.max_per_category))
    df = pd.concat(keep, ignore_index=True).drop_duplicates(subset=["URL"])

    sess = build_session(cache=True)
    robots_cache = {}

    # prepare outputs
    log_f = open(args.log_csv, "w", newline="", encoding="utf-8")
    log_w = csv.DictWriter(log_f, fieldnames=[
        "URL","Status","Reason","Category","Source_Domain","Title","Publish_Date",
        "html_path","pdf_path","txt_path","sha256","bytes","fetched_at"
    ])
    log_w.writeheader()

    jsonl_f = open(args.jsonl_path, "a", encoding="utf-8")

    pbar = tqdm(df.itertuples(index=False), total=len(df), desc="Scraping")
    for row in pbar:
        url = getattr(row, "URL", "")
        cat = getattr(row, "Category_Guess", "")
        src = getattr(row, "Source_Domain", "")
        ttl = getattr(row, "Title", "")
        pdate = getattr(row, "Publish_Date", "")

        if not url:
            log_w.writerow({"URL": url, "Status":"skip", "Reason":"no_url", "Category": cat, "Source_Domain": src, "Title": ttl, "Publish_Date": pdate})
            continue

        # robots
        if not args.ignore_robots and not allowed_by_robots(robots_cache, url, UA):
            log_w.writerow({"URL": url, "Status":"blocked", "Reason":"robots.txt", "Category": cat,
                            "Source_Domain": src, "Title": ttl, "Publish_Date": pdate})
            time.sleep(args.throttle_sec)
            continue

        try:
            resp = sess.get(url, timeout=25)
        except Exception as e:
            log_w.writerow({"URL": url, "Status":"error", "Reason": f"request:{e}", "Category": cat,
                            "Source_Domain": src, "Title": ttl, "Publish_Date": pdate})
            time.sleep(args.throttle_sec); continue

        html_path = pdf_path = txt_path = ""
        text_out = ""
        raw_bytes = b""
        status = "ok"; reason = ""
        try:
            if is_pdf_response(resp, url):
                raw_bytes = resp.content or b""
                if not raw_bytes:
                    status, reason = "error", "empty_pdf"
                else:
                    fid = sha256_bytes(raw_bytes)[:16]
                    pdf_path = str(artifacts / "pdf" / f"{fid}.pdf")
                    with open(pdf_path, "wb") as f: f.write(raw_bytes)
                    text_out = pdf_bytes_to_text(raw_bytes)
            else:
                # HTML
                html = resp.text or ""
                if not html.strip():
                    status, reason = "error", "empty_html"
                else:
                    fid = sha256_text(url)[:16]
                    html_path = str(artifacts / "html" / f"{fid}.html")
                    with open(html_path, "w", encoding="utf-8") as f: f.write(html)
                    text_out = clean_html_to_text(html, base_url=url)

            if text_out:
                fid_txt = sha256_text(text_out)[:16]
                txt_path = str(artifacts / "txt" / f"{fid_txt}.txt")
                with open(txt_path, "w", encoding="utf-8") as f: f.write(text_out)
                doc_sha = sha256_text(text_out)
            else:
                doc_sha = ""
                if status == "ok":  # no earlier error set
                    status, reason = "warn", "no_text_extracted"

            # write JSONL record
            rec = {
                "url": url,
                "title": ttl,
                "publish_date": pdate,
                "source_domain": src,
                "category": cat,
                "fetched_at": datetime.utcnow().isoformat() + "Z",
                "html_path": html_path or None,
                "pdf_path": pdf_path or None,
                "txt_path": txt_path or None,
                "sha256": doc_sha,
                "bytes": len(raw_bytes) if raw_bytes else None,
                "status": status,
                "reason": reason or None,
            }
            jsonl_f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            jsonl_f.flush()

            # log csv
            log_w.writerow({
                "URL": url, "Status": status, "Reason": reason, "Category": cat,
                "Source_Domain": src, "Title": ttl, "Publish_Date": pdate,
                "html_path": html_path, "pdf_path": pdf_path, "txt_path": txt_path,
                "sha256": doc_sha, "bytes": len(raw_bytes) if raw_bytes else 0,
                "fetched_at": rec["fetched_at"]
            })

        except Exception as e:
            log_w.writerow({"URL": url, "Status":"error", "Reason": f"processing:{e}", "Category": cat,
                            "Source_Domain": src, "Title": ttl, "Publish_Date": pdate})
        finally:
            time.sleep(args.throttle_sec)

    log_f.close(); jsonl_f.close()
    print(f"[DONE] Log: {args.log_csv}")
    print(f"[DONE] JSONL corpus: {args.jsonl_path}")
    print(f"[DONE] Artifacts in: {artifacts}")

if __name__ == "__main__":
    main()

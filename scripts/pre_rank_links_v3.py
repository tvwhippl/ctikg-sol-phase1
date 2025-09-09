#!/usr/bin/env python3
import argparse, json, math, re, sys, csv
from datetime import datetime, timezone
from urllib.parse import urljoin
import requests
import feedparser
from dateutil import parser as dateparser

UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}
CVE_RE = re.compile(r"CVE-\d{4}-\d{4,7}", re.I)
MITRE_T_RE = re.compile(r"\bT\d{4}\b")
IOC_TOKENS = ["sha256", "md5", "indicator", "ioc", "hash", "domain", "ip address"]

def now_utc(): return datetime.now(timezone.utc)

def parse_date(s):
    if not s:
        return None
    try:
        dt = dateparser.parse(s)
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None

def recency_score(pub_dt, half_life_days):
    if not pub_dt:
        return 0.4
    days = (now_utc() - pub_dt).days
    lam = math.log(2) / max(1, half_life_days)
    return math.exp(-lam * max(days, 0))

def keyword_score(text, category):
    text_l = (text or "").lower()
    inc = category.get("include", [])
    exc = category.get("exclude", [])
    inc_hits = sum(1 for w in inc if w in text_l)
    exc_hits = sum(1 for w in exc if w in text_l)
    return max(0, inc_hits - exc_hits)

def signal_score(text):
    s = 0
    if CVE_RE.search(text or ""): s += 2
    if MITRE_T_RE.search(text or ""): s += 1
    tl = (text or "").lower()
    s += sum(1 for tok in IOC_TOKENS if tok in tl)
    return s

def pick_category(title, summary, categories):
    best, best_score = None, -1
    combined = " ".join([title or "", summary or ""])
    for name, cat_def in categories.items():
        k = keyword_score(combined, cat_def)
        if k > best_score:
            best_score, best = k, name
    return best, best_score

def fetch_feed(feed_url, verbose=False, timeout=25):
    r = requests.get(feed_url, headers=UA, timeout=timeout, allow_redirects=True)
    status = r.status_code
    d = feedparser.parse(r.content) if r.ok else feedparser.parse(feed_url)
    if verbose:
        print(f"[FEED] {feed_url} -> HTTP {status}; entries={len(getattr(d,'entries',[]))}; bozo={getattr(d,'bozo',False)}")
    return d

def crawl_index(url, base=None, link_pattern=None, date_regex=None, verbose=False, timeout=25):
    """
    Lightweight index crawler: fetch HTML and pull links matching regex.
    We *do not* fetch article bodies here.
    """
    try:
        r = requests.get(url, headers=UA, timeout=timeout, allow_redirects=True)
        r.raise_for_status()
        html = r.text
        links = []
        lp = re.compile(link_pattern) if link_pattern else None
        for m in re.finditer(r'href=["\']([^"\']+)["\']', html, flags=re.I):
            href = m.group(1)
            if lp and not lp.search(href):
                continue
            full = urljoin(base or url, href)
            # crude title guess near the anchor tag
            start = max(0, m.start()-120)
            end = min(len(html), m.end()+120)
            snippet = html[start:end]
            # title heuristic
            tmatch = re.search(r'>([^<>]{3,120})<', snippet)
            title = (tmatch.group(1).strip() if tmatch else href).replace('\n',' ').strip()
            # date heuristic
            pub_dt = None
            if date_regex:
                dmatch = re.search(date_regex, snippet)
                if dmatch:
                    pub_dt = parse_date(dmatch.group(1))
            links.append((full, title, pub_dt))
        if verbose:
            print(f"[INDEX] {url} -> {len(links)} candidates")
        return links
    except Exception as e:
        if verbose:
            print(f"[INDEX-ERR] {url}: {e}")
        return []

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sources", default="Sources_Config_Expanded.json")
    ap.add_argument("--categories", default="Category_Keywords_Expanded.json")
    ap.add_argument("--out", default="Links_Queue.csv")
    ap.add_argument("--limit_per_feed", type=int, default=0)
    ap.add_argument("--half_life_days", type=int, default=180, help="Recency half-life (default 180 days)")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    with open(args.sources, "r") as f: sources = json.load(f)
    with open(args.categories, "r") as f: categories = json.load(f)

    rows, seen = [], set()

    for dom, cfg in sources.get("domains", {}).items():
        weight = float(cfg.get("weight", 0.5))
        if args.verbose:
            print(f"\n[DOMAIN] {dom} w={weight}")
        # RSS feeds
        for feed_url in cfg.get("rss", []):
            try:
                d = fetch_feed(feed_url, verbose=args.verbose)
                entries = getattr(d, "entries", [])
                if args.limit_per_feed and len(entries) > args.limit_per_feed:
                    entries = entries[:args.limit_per_feed]
                for e in entries:
                    link = e.get("link");  title = e.get("title","")
                    if not link or link in seen: continue
                    seen.add(link)
                    summary = e.get("summary","") or e.get("description","")
                    pub = e.get("published") or e.get("updated") or e.get("created") or ""
                    pub_dt = parse_date(pub)
                    cat_guess, cat_hits = pick_category(title, summary, categories)
                    rscore = recency_score(pub_dt, args.half_life_days)
                    sscore = signal_score(" ".join([title, summary]))
                    score = (0.35*weight)+(0.30*rscore)+(0.25*(cat_hits/3.0))+(0.10*min(sscore/3.0,1.0))
                    rows.append({
                        "ID":"", "URL":link, "Source_Domain":dom, "Source_Type":"RSS",
                        "Title":title, "Snippet":summary, "Publish_Date":pub_dt.isoformat() if pub_dt else "",
                        "Category_Guess":cat_guess or "", "Score":round(score,4),
                        "Reason":f"dom_w={weight}, rec={round(rscore,2)}, cat_hits={cat_hits}, sig={sscore}",
                        "Status":"New", "Collected_By":"", "Added_On":datetime.utcnow().isoformat(), "Last_Checked":""
                    })
            except Exception as ex:
                if args.verbose: print(f"[FEED-ERR] {feed_url}: {ex}")
        # HTML index pages
        for entry in cfg.get("indexes", []):
            url = entry.get("url"); base = entry.get("base"); link_pat = entry.get("link_pattern"); date_re = entry.get("date_regex")
            candidates = crawl_index(url, base, link_pat, date_re, verbose=args.verbose)
            for link, title, pub_dt in candidates:
                if link in seen: continue
                seen.add(link)
                summary = ""
                cat_guess, cat_hits = pick_category(title, summary, categories)
                rscore = recency_score(pub_dt, args.half_life_days)
                sscore = signal_score(title)
                score = (0.35*weight)+(0.30*rscore)+(0.25*(cat_hits/3.0))+(0.10*min(sscore/3.0,1.0))
                rows.append({
                    "ID":"", "URL":link, "Source_Domain":dom, "Source_Type":"INDEX",
                    "Title":title, "Snippet":summary, "Publish_Date":pub_dt.isoformat() if pub_dt else "",
                    "Category_Guess":cat_guess or "", "Score":round(score,4),
                    "Reason":f"dom_w={weight}, rec={round(rscore,2)}, cat_hits={cat_hits}, sig={sscore}",
                    "Status":"New", "Collected_By":"", "Added_On":datetime.utcnow().isoformat(), "Last_Checked":""
                })

    rows.sort(key=lambda r: r["Score"], reverse=True)
    out_cols = ["ID","URL","Source_Domain","Source_Type","Title","Snippet","Publish_Date",
                "Category_Guess","Score","Reason","Status","Collected_By","Added_On","Last_Checked"]
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        import csv
        w = csv.DictWriter(f, fieldnames=out_cols); w.writeheader()
        for r in rows: w.writerow(r)
    print(f"\n[DONE] Wrote {len(rows)} rows to {args.out}")
if __name__ == "__main__":
    from datetime import datetime
    main()

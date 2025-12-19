#!/usr/bin/env python3
import argparse, csv, glob, os, sys
from urllib.parse import urlparse

def normkey(k): return (k or "").strip().lower().replace(" ", "_")

def load_rows(paths):
    seen, rows = set(), []
    for p in paths:
        if not os.path.exists(p): 
            continue
        with open(p, newline="", encoding="utf-8", errors="ignore") as f:
            r = csv.DictReader(f)
            cols = [normkey(c) for c in (r.fieldnames or [])]
            for row in r:
                d = {normkey(k): (v or "").strip() for k, v in row.items()}
                url = d.get("url") or d.get("link") or d.get("article_url") or d.get("page")
                if not (isinstance(url, str) and url.startswith("http")):
                    continue
                if url in seen:
                    continue
                seen.add(url)
                cat = d.get("category") or d.get("topic") or "unspecified"
                ttl = d.get("title") or d.get("headline") or ""
                sd  = d.get("source_domain") or urlparse(url).netloc
                rows.append({"url": url, "category": cat, "title": ttl, "source_domain": sd})
    return rows

def write_csv(path, rows):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["url","category","title","source_domain"])
        w.writeheader()
        for r in rows: w.writerow(r)

def main():
    ap = argparse.ArgumentParser()

    # Optional modes
    ap.add_argument("--glob", default=None, help="glob of input CSVs (alt mode)")
    ap.add_argument("--out", default=None, help="output CSV (alt mode)")

    # CRITICAL: define the flag HERE
    ap.add_argument(
        "--no-clobber-batch",
        action="store_true",
        help="Do not overwrite batch_topic.csv if it already exists",
    )

    # Compat mode: 3 positional args
    ap.add_argument(
        "compat",
        nargs="*",
        help="[in_master.csv out_links.csv out_batch.csv]",
    )

    args = ap.parse_args()

    # ---- compat mode ----
    if len(args.compat) == 3:
        in_master, out_links, out_batch = args.compat

        rows = load_rows([in_master, out_batch])

        # Always write merged queue
        write_csv(out_links, rows)

        # Conditionally write batch
        if args.no_clobber_batch and os.path.exists(out_batch):
            print(
                f"[OK] merge_dedup compat: {in_master} -> {len(rows)} rows "
                f"-> {out_links}; skipped overwriting {out_batch} (no-clobber)"
            )
        else:
            write_csv(out_batch, rows)
            print(
                f"[OK] merge_dedup compat: {in_master} -> {len(rows)} rows "
                f"-> {out_links}, {out_batch}"
            )
        return

    # ---- alt mode (unchanged) ----
    if args.glob and args.out:
        paths = glob.glob(args.glob)
        rows = load_rows(paths)
        write_csv(args.out, rows)
        print(f"[OK] merge_dedup glob: {len(paths)} files -> {len(rows)} rows -> {args.out}")
        return

    ap.error("Invalid arguments: use compat mode (3 files) or --glob/--out")

if __name__ == "__main__":
    main()

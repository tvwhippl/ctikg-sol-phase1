#!/usr/bin/env python3
"""scripts/make_helper_flags.py

Adds quick helper flags to a link-queue CSV.

Legacy behavior (kept):
  python3 scripts/make_helper_flags.py data/Links_Queue.csv

  - writes: data/Links_Queue_sorted_flags.csv
  - writes: Triage_*_top200.csv and Suggested_Selected_master.csv in repo root

New behavior (for per-run open-topic isolation):
  python3 scripts/make_helper_flags.py --in <queue.csv> --out <flags.csv> --no-triage

This lets `make open-topic` write run-local queue artifacts without polluting the repo root.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import pandas as pd


def canon_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names from diverse inputs.

    - Maps common synonyms to canonical lower-case: url, title, category, source_domain
    - Lower-cases all columns
    - Adds back-compat uppercase aliases (URL, Title, Category, Source_Domain)
      so existing code that selects by those names keeps working.
    """

    # map common synonyms -> canonical lower-case
    rename = {}
    for c in list(df.columns):
        lc = c.strip().lower()
        if lc in {"url", "link", "href", "article_url", "page"}:
            rename[c] = "url"
        elif lc in {"title", "headline"}:
            rename[c] = "title"
        elif lc in {"category", "topic", "tag"}:
            rename[c] = "category"
        elif lc in {"source_domain", "domain", "site", "host"}:
            rename[c] = "source_domain"

    df = df.rename(columns=rename)
    df.columns = [x.strip().lower() for x in df.columns]

    # Back-compat aliases so existing code that expects upper-case keeps working
    if "url" in df.columns and "URL" not in df.columns:
        df["URL"] = df["url"]
    if "title" in df.columns and "Title" not in df.columns:
        df["Title"] = df["title"]
    if "category" in df.columns and "Category" not in df.columns:
        df["Category"] = df["category"]
    if "source_domain" in df.columns and "Source_Domain" not in df.columns:
        df["Source_Domain"] = df["source_domain"]

    return df


# Monkey-patch pd.read_csv so every CSV read is normalized
_pd_read_csv = pd.read_csv


def _read_csv_canon(*args, **kwargs):
    kwargs.setdefault("dtype", str)
    kwargs.setdefault("keep_default_na", False)
    return canon_cols(_pd_read_csv(*args, **kwargs))


pd.read_csv = _read_csv_canon


def _compute_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Compute cheap quality flags used by selection."""

    df = df.copy()

    # ensure columns exist
    for col in ["Title", "Snippet", "Source_Domain", "Category_Guess", "Score", "Publish_Date", "Status"]:
        if col not in df.columns:
            df[col] = ""

    # normalize
    df["Title"] = df["Title"].astype(str).fillna("")
    df["Snippet"] = df["Snippet"].astype(str).fillna("")
    df["Source_Domain"] = df["Source_Domain"].astype(str).fillna("")
    df["Category_Guess"] = df["Category_Guess"].astype(str).fillna("")
    df["Score"] = pd.to_numeric(df["Score"], errors="coerce")
    text = (df["Title"] + " " + df["Snippet"]).str.lower()

    # reputable domains
    reputable = [
        "cisa.gov",
        "paloaltonetworks.com",
        "talosintelligence.com",
        "crowdstrike.com",
        "microsoft.com",
        "elastic.co",
        "redcanary.com",
        "securelist.com",
        "ubuntu.com",
        "access.redhat.com",
        "suse.com",
        "openssh.com",
        "krebsonsecurity.com",
        "schneier.com",
        "thedfirreport.com",
        "sysdig.com",
        "sentinelone.com",
        "trendmicro.com",
        "unit42.paloaltonetworks.com",
        "zeek.org",
        "suricata.io",
        "openondemand.org",
        "apptainer.org",
        "cloud.google.com",
        "rapid7.com",
        "qualys.com",
        "huntress.com",
        "uptycs.com",
        "beegfs.io",
        "lore.kernel.org",
        "lists.lustre.org",
        "darkreading.com",
        "threatpost.com",
        "zdnet.com",
        "scmagazine.com",
        "bankinfosecurity.com",
        "infosecurity-magazine.com",
        "bleepingcomputer.com",
    ]
    df["RepFlag"] = df["Source_Domain"].str.lower().apply(lambda d: int(any(dom in d for dom in reputable)))

    # signal tokens
    CVE_RE = re.compile(r"cve-\d{4}-\d{4,7}", re.I)
    TID_RE = re.compile(r"\bT\d{4}\b")
    iotoks = ["sha256", "md5", "ioc", "indicator", "ip address", "hash", "domain", "url"]
    mining = ["xmrig", "xmr-stak", "stratum", "monero", "nicehash"]
    nfs = [
        "nfs",
        "lustre",
        "gpfs",
        "beegfs",
        "root_squash",
        "no_root_squash",
        "/etc/exports",
        "ganesha",
        "krb5",
        "krb5p",
        "krb5i",
    ]
    ssh = [
        "ssh ",
        "sshd",
        "authorized_keys",
        "known_hosts",
        "kerberos",
        "gssapi",
        "password spraying",
        "credential stuffing",
    ]

    df["has_CVE"] = text.apply(lambda t: int(bool(CVE_RE.search(t))))
    df["has_TID"] = text.apply(lambda t: int(bool(TID_RE.search(t))))
    df["has_IOC"] = text.apply(lambda t: int(any(tok in t for tok in iotoks)))
    df["has_MiningTok"] = text.apply(lambda t: int(any(tok in t for tok in mining)))
    df["has_NFSTok"] = text.apply(lambda t: int(any(tok in t for tok in nfs)))
    df["has_SSHTok"] = text.apply(lambda t: int(any(tok in t for tok in ssh)))

    # flags/scores
    df["SigFlag"] = (
        (
            df["has_CVE"]
            + df["has_TID"]
            + df["has_IOC"]
            + df["has_MiningTok"]
            + df["has_NFSTok"]
            + df["has_SSHTok"]
        )
        > 0
    ).astype(int)
    df["Quality2"] = df["RepFlag"] + df["SigFlag"]  # 0..2
    df["Quality4"] = df["RepFlag"] + df["has_CVE"] + df["has_TID"] + df["has_IOC"]  # 0..4

    return df


def _write_triage_packs(df: pd.DataFrame, triage_dir: Path) -> None:
    """Legacy triage packs: useful for manual inspection (writes many files)."""

    triage_dir.mkdir(parents=True, exist_ok=True)

    def triage(cat: str, topn: int = 200) -> None:
        sub = df[df["Category_Guess"] == cat].copy()
        if sub.empty:
            print(f"[INFO] triage: '{cat}' has 0 rows; skipping")
            return

        sub = sub.sort_values("Score", ascending=False)

        cols = [
            c
            for c in (
                "URL",
                "Title",
                "Source_Domain",
                "Publish_Date",
                "Score",
                "RepFlag",
                "SigFlag",
                "Quality2",
                "Quality4",
                "has_CVE",
                "has_TID",
                "has_IOC",
            )
            if c in sub.columns
        ]

        safe_cat = cat.replace("/", "_").replace(" ", "_")
        outp = triage_dir / f"Triage_{safe_cat}_top{topn}.csv"
        sub[cols].head(topn).to_csv(outp, index=False)
        print("Wrote", str(outp), "rows:", min(len(sub), topn))

    # Keep the existing category list (legacy)
    cats = [
        "SSH & Credential Abuse",
        "Cryptomining on HPC",
        "NFS / File–Share Exposure",
        "JupyterHub / Open OnDemand",
    ]

    for c in cats:
        triage(c, 200)

    # Suggested_Selected_master.csv
    suggested = []
    for c in cats:
        sub = df[df["Category_Guess"] == c].copy()
        if sub.empty:
            continue

        sub = sub.sort_values("Score", ascending=False)

        strong = sub[sub["Quality4"] >= 2].head(120)
        if len(strong) < 120:
            strong = pd.concat([strong, sub.head(120)], ignore_index=True)
            strong = strong.drop_duplicates(subset=["URL"])

        if strong.empty:
            continue

        strong["Status"] = "Selected"

        cols = [
            c
            for c in (
                "URL",
                "Title",
                "Source_Domain",
                "Category_Guess",
                "Publish_Date",
                "Score",
                "RepFlag",
                "SigFlag",
                "Quality2",
                "Quality4",
                "Status",
            )
            if c in strong.columns
        ]

        suggested.append(strong.reindex(columns=cols))

    out_master = triage_dir / "Suggested_Selected_master.csv"
    if suggested:
        out_df = pd.concat(suggested, ignore_index=True).drop_duplicates("URL")
        out_df.to_csv(out_master, index=False)
        print("Wrote", str(out_master), "with", len(out_df), "rows")
    else:
        out_master.write_text("", encoding="utf-8")
        print("[WARN] no rows ->", str(out_master), "(empty)")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Add helper flags to a link queue CSV")
    ap.add_argument("--in", dest="in_path", default=None, help="Input queue CSV path")
    ap.add_argument(
        "--out",
        dest="out_path",
        default="data/Links_Queue_sorted_flags.csv",
        help="Output flags CSV path (default: data/Links_Queue_sorted_flags.csv)",
    )
    ap.add_argument(
        "--no-triage",
        action="store_true",
        help="Do not write triage packs (Triage_*.csv / Suggested_Selected_master.csv)",
    )
    ap.add_argument(
        "--triage-dir",
        default=".",
        help="Directory to write triage packs (default: repo root). Ignored with --no-triage.",
    )
    ap.add_argument("pos_in", nargs="?", help="(legacy) positional input CSV")

    args = ap.parse_args(argv)

    in_path = args.in_path or args.pos_in or "data/Links_Queue.csv"
    out_path = Path(args.out_path)

    df = pd.read_csv(in_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if df.empty:
        df.to_csv(out_path, index=False)
        print(f"[WARN] {in_path} is empty; wrote 0 rows to {out_path} and exiting")
        return 0

    df = _compute_flags(df)
    df.to_csv(out_path, index=False)
    print("Wrote", str(out_path), "with", len(df), "rows")

    if not args.no_triage:
        _write_triage_packs(df, triage_dir=Path(args.triage_dir))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# Phase 1: Article Assembly & Category-Driven Pre‑Ranking

**Project:** CIREN Facilitator — CTI Knowledge Graph (CTIKG) bootstrapping for SOL HPC security

**Authors:** Trevor Whipple + ChatGPT 5 Thinking

**Date:** 9-10-25

## 1. Objectives

We designed a reproducible pipeline to assemble, pre‑rank, and curate cybersecurity articles relevant to an HPC environment like SOL. The outputs will seed a simple, testable CTI knowledge graph (CTIKG) focused on four operational categories:
- SSH & Credential Abuse
- Cryptomining on HPC
- NFS / File‑Share Exposure
- JupyterHub / Open OnDemand

## 2. Environment & Prerequisites
- macOS + Python 3.10/3.11
- Packages: `requests`, `feedparser`, `python-dateutil`, `pandas` (optional)
- Working folder: `link queue/`

## 3. Data Sources & Categories

- Source list: `Sources_Config_Expanded_v2.json` (RSS feeds + HTML index pages; weighted by domain)

- Category keywords: `Category_Keywords_Expanded.json` (include/exclude terms per category)

- We favor reputable advisories, vendor IR blogs, and core project/distro pages. News sites added at lower weight.

## 4. Pipeline Overview

1. **Pre‑ranking (no full scraping):** `pre_rank_links_v3.py` collects recent/history from RSS and select HTML indexes, generating `Links_Queue.csv` with lightweight metadata.

2. **Merge & de‑dupe:** `merge_dedupe.py` combines batches and drops duplicate URLs.

3. **Flagging for triage:** `make_helper_flags.py` adds `RepFlag`, `SigFlag`, and composite `Quality2`/`Quality4` signals.

4. **Selection:** `select_winners.py` picks N winners per category into per‑category CSVs + a master list.

### 4.1 Scoring function used during pre‑ranking

For each feed item we compute:
- `recency_score` via exponential decay with a configurable half‑life (30–9999 days).
- `category_hits` = keyword matches in title/summary.
- `signal_score` = presence of CVE IDs / MITRE T‑IDs / IOC tokens.
- `domain_weight` from the source config.

The final score (0–1-ish) is:

`Score = 0.35*domain_weight + 0.30*recency_score + 0.25*(category_hits/3) + 0.10*min(signal_score/3, 1)`

We use this only to prioritize; we don’t fetch article bodies at this stage.

### 4.2 Selection policy (winners)

We target 100 winners per category. Items are ranked by `Quality4`, then `Quality2`, `RepFlag`, `SigFlag`, and `Score`. We prioritize:
- Reputable domains (advisories/projects/vendor IR).
- Concrete signals (CVEs, T‑IDs, IOC terms).
- High pre‑rank scores.
Rows marked `Status=Rejected` are never selected unless explicitly requested.

## 5. Reproducibility — exact commands

From inside the project folder:
```bash
# 1) Install deps
python3 -m pip install --upgrade pip
python3 -m pip install requests feedparser python-dateutil pandas

# 2) Pre-rank (big sweep)
python3 pre_rank_links_v3.py \
  --sources Sources_Config_Expanded_v2.json \
  --categories Category_Keywords_Expanded.json \
  --out batch_extra.csv \
  --limit_per_feed 500 \
  --half_life_days 9999 \
  --verbose

# 3) Merge & de-dupe into working queue
python3 merge_dedupe.py Links_Queue_master.csv Links_Queue.csv batch*.csv
mv Links_Queue_master.csv Links_Queue.csv

# 4) Sort by Score (optional view)
python3 - << 'PY'
import pandas as pd
df=pd.read_csv('Links_Queue.csv')
df['Score']=pd.to_numeric(df['Score'],errors='coerce')
df.sort_values('Score',ascending=False).to_csv('Links_Queue_sorted.csv',index=False)
print('Wrote Links_Queue_sorted.csv with',len(df),'rows.')
PY

# 5) Add flags
python3 make_helper_flags.py Links_Queue_sorted.csv

# 6) Auto-select winners (100/category by default)
python3 select_winners.py \
  --in Links_Queue_sorted_flags.csv \
  --out Links_Queue_with_selected.csv
```
## 6. Results

- Queue size (pre‑dedupe): **889** rows.

- Queue size (unique URLs): **889** rows.

### 6.1 Category distribution in the queue

| Category                   |   Count |
|:---------------------------|--------:|
| SSH & Credential Abuse     |     704 |
| JupyterHub / Open OnDemand |     111 |
| NFS / File-Share Exposure  |      71 |
| Cryptomining on HPC        |       3 |


### 6.2 Winners selected per category

| Category                   |   SelectedCount |
|:---------------------------|----------------:|
| SSH & Credential Abuse     |             100 |
| JupyterHub / Open OnDemand |             100 |
| NFS / File-Share Exposure  |              71 |
| Cryptomining on HPC        |               3 |


### 6.3 Top source domains among winners

| Source_Domain                  |   Count |
|:-------------------------------|--------:|
| www.huntress.com               |     125 |
| unit42.paloaltonetworks.com    |      17 |
| blog.talosintelligence.com     |      16 |
| ubuntu.com                     |      11 |
| www.uptycs.com                 |      10 |
| www.crowdstrike.com            |      10 |
| www.darkreading.com            |      10 |
| www.microsoft.com              |      10 |
| thedfirreport.com              |       9 |
| jupyter.org                    |       9 |
| googleprojectzero.blogspot.com |       9 |
| www.schneier.com               |       5 |


### 6.4 Quality signals among winners

- 92.3% of winners came from reputable sources
- 24.8% of winners contained signal tokens (CVE/IOC/etc.)

## 7. Rationale for selection

We selected items that are directly actionable for SOC workflows on SOL:
- Clear mapping to one of the four categories (SSH, cryptomining, NFS, Jupyter/OnDemand).
- Concrete artifacts (CVE IDs, hashes, domains/IPs), procedures, or defensive guidance.
- Coverage from core projects and distributions (OpenSSH, Ubuntu/Red Hat/SUSE), and IR teams with demonstrated telemetry.

This balances breadth (news and general analysis at lower weights) with depth (advisories and post‑mortems at higher weights).

## 8. Limitations & Next Steps

- RSS coverage varies; some projects require HTML index parsing (we avoid full scraping at this stage).
- Publication bias toward English sources; consider adding regional feeds if relevant.
- The pre‑ranker uses simple keyword matching. For CTIKG ingestion, we will extract entities (CVE, TTP, IOC) from **full text** of the Selected set.

**Next:** run the winner‑scraper to fetch PDFs + cleaned text for `Status=Selected`, then convert to CTIKG JSONL and link to actual SOL logs for a pilot evaluation.

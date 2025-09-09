# CTIKG for SOL — Phase 1

Reproducible pipeline to collect, pre-rank, and select cybersecurity articles for:
1) SSH & Credential Abuse  
2) Cryptomining on HPC  
3) NFS / File-Share Exposure  
4) JupyterHub / Open OnDemand

## Quick start

```bash
python3 -m pip install -r requirements.txt

python3 scripts/pre_rank_links_v3.py \
  --sources configs/Sources_Config_Expanded_v2.json \
  --categories configs/Category_Keywords_Expanded.json \
  --out batch_extra.csv \
  --limit_per_feed 500 \
  --half_life_days 9999 \
  --verbose

python3 scripts/merge_dedupe.py data/Links_Queue_master.csv data/Links_Queue.csv batch_extra.csv
mv data/Links_Queue_master.csv data/Links_Queue.csv

python3 scripts/make_helper_flags.py data/Links_Queue_sorted.csv

python3 scripts/select_winners.py \
  --in data/Links_Queue_sorted_flags.csv \
  --out data/Links_Queue_with_selected.csv

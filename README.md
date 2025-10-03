# CTIKG for SOL — Phase 1

Reproducible pipeline to collect, pre-rank, and select cybersecurity articles for:
1) SSH & Credential Abuse  
2) Cryptomining on HPC  
3) NFS / File-Share Exposure  
4) JupyterHub / Open OnDemand

## Open-Topic Phase-1 (local dev)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

make topic-setup
make topic-gen TOPIC="CI/CD pipeline attacks: runner poisoning, OIDC, artifact/cache poisoning"
make topic-pull SOURCES=configs/sources/common.json
make topic-select
make topic-scrape WINNERS=25 CONCURRENCY=2 THROTTLE_SEC=1 IGNORE_ROBOTS=1
make topic-chunk
make topic-verify


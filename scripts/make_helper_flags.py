#!/usr/bin/env python3
import pandas as pd, re, os, sys

in_path = sys.argv[1] if len(sys.argv) > 1 else "Links_Queue_sorted.csv"
df = pd.read_csv(in_path)

# ensure columns exist
for col in ["Title","Snippet","Source_Domain","Category_Guess","Score","Publish_Date","Status"]:
    if col not in df.columns: df[col] = ""

# normalize
df["Title"] = df["Title"].astype(str).fillna("")
df["Snippet"] = df["Snippet"].astype(str).fillna("")
df["Source_Domain"] = df["Source_Domain"].astype(str).fillna("")
df["Category_Guess"] = df["Category_Guess"].astype(str).fillna("")
df["Score"] = pd.to_numeric(df["Score"], errors="coerce")
text = (df["Title"] + " " + df["Snippet"]).str.lower()

# reputable domains
reputable = [
    "cisa.gov","paloaltonetworks.com","talosintelligence.com","crowdstrike.com",
    "microsoft.com","elastic.co","redcanary.com","securelist.com","ubuntu.com",
    "access.redhat.com","suse.com","openssh.com","krebsonsecurity.com","schneier.com",
    "thedfirreport.com","sysdig.com","sentinelone.com","trendmicro.com",
    "unit42.paloaltonetworks.com","zeek.org","suricata.io","openondemand.org",
    "apptainer.org","cloud.google.com","rapid7.com","qualys.com","huntress.com",
    "uptycs.com","beegfs.io","lore.kernel.org","lists.lustre.org","darkreading.com",
    "threatpost.com","zdnet.com","scmagazine.com","bankinfosecurity.com",
    "infosecurity-magazine.com","bleepingcomputer.com"
]
df["RepFlag"] = df["Source_Domain"].str.lower().apply(lambda d: int(any(dom in d for dom in reputable)))

# signal tokens
CVE_RE = re.compile(r"cve-\d{4}-\d{4,7}", re.I)
TID_RE = re.compile(r"\bT\d{4}\b")
iotoks = ["sha256","md5","ioc","indicator","ip address","hash","domain","url"]
mining = ["xmrig","xmr-stak","stratum","monero","nicehash"]
nfs    = ["nfs","lustre","gpfs","beegfs","root_squash","no_root_squash","/etc/exports","ganesha","krb5","krb5p","krb5i"]
ssh    = ["ssh ","sshd","authorized_keys","known_hosts","kerberos","gssapi","password spraying","credential stuffing"]

df["has_CVE"]      = text.apply(lambda t: int(bool(CVE_RE.search(t))))
df["has_TID"]      = text.apply(lambda t: int(bool(TID_RE.search(t))))
df["has_IOC"]      = text.apply(lambda t: int(any(tok in t for tok in iotoks)))
df["has_MiningTok"]= text.apply(lambda t: int(any(tok in t for tok in mining)))
df["has_NFSTok"]   = text.apply(lambda t: int(any(tok in t for tok in nfs)))
df["has_SSHTok"]   = text.apply(lambda t: int(any(tok in t for tok in ssh)))

# flags/scores
df["SigFlag"]  = ((df["has_CVE"]+df["has_TID"]+df["has_IOC"]+df["has_MiningTok"]+df["has_NFSTok"]+df["has_SSHTok"])>0).astype(int)
df["Quality2"] = df["RepFlag"] + df["SigFlag"]                      # 0..2
df["Quality4"] = df["RepFlag"] + df["has_CVE"] + df["has_TID"] + df["has_IOC"]  # 0..4

# save flags file
flags_path = "Links_Queue_sorted_flags.csv"
df.to_csv(flags_path, index=False)
print("Wrote", flags_path, "with", len(df), "rows")

# triage packs per category
def triage(cat, topn=200):
    sub = df[df["Category_Guess"] == cat].copy().sort_values("Score", ascending=False)
    cols = ["URL","Title","Source_Domain","Publish_Date","Score","RepFlag","SigFlag","Quality2","Quality4","has_CVE","has_TID","has_IOC"]
    for c in cols:
        if c not in sub.columns: sub[c] = ""
    outp = f"Triage_{cat.replace('/','_').replace(' ','_')}_top{topn}.csv"
    sub[cols].head(topn).to_csv(outp, index=False)
    print("Wrote", outp, "rows:", min(len(sub), topn))

cats = ["SSH & Credential Abuse","Cryptomining on HPC","NFS / File-Share Exposure","JupyterHub / Open OnDemand"]
for c in cats: triage(c, 200)

# suggested Selected master
suggested = []
for c in cats:
    sub = df[df["Category_Guess"] == c].copy().sort_values("Score", ascending=False)
    strong = sub[sub["Quality4"] >= 2].head(120)
    if len(strong) < 120:
        strong = pd.concat([strong, sub.head(120)], ignore_index=True).drop_duplicates(subset=["URL"])
    strong["Status"] = "Selected"
    cols = ["URL","Title","Source_Domain","Category_Guess","Publish_Date","Score","RepFlag","SigFlag","Quality2","Quality4","Status"]
    suggested.append(strong[cols])
pd.concat(suggested, ignore_index=True).drop_duplicates("URL").to_csv("Suggested_Selected_master.csv", index=False)
print("Wrote Suggested_Selected_master.csv")

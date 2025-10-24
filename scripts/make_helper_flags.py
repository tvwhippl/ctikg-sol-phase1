#!/usr/bin/env python3
import pandas as pd, re, os, sys

def canon_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names from diverse inputs.

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

# ---- read input right here ----
in_path   = sys.argv[1] if len(sys.argv) > 1 else "data/Links_Queue.csv"
flags_path = "data/Links_Queue_sorted_flags.csv"

df = pd.read_csv(in_path)         # <= THIS is the place to read
# print(f"[INFO] loaded {len(df)} rows from {in_path}")  # optional

if df.empty:
    # still write the flags file so downstream steps don’t crash
    df.to_csv(flags_path, index=False)
    print(f"[WARN] {in_path} is empty; wrote 0 rows to {flags_path} and exiting")
    sys.exit(0)

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
df.to_csv(flags_path, index=False)
print("Wrote", flags_path, "with", len(df), "rows")

# -----------------------------
# triage packs per category
# -----------------------------
def triage(cat, topn=200):
    sub = df[df["Category_Guess"] == cat].copy()
    if sub.empty:
        print(f"[INFO] triage: '{cat}' has 0 rows; skipping")
        return

    sub = sub.sort_values("Score", ascending=False)

    cols = [c for c in (
        "URL","Title","Source_Domain","Publish_Date","Score",
        "RepFlag","SigFlag","Quality2","Quality4","has_CVE","has_TID","has_IOC"
    ) if c in sub.columns]

    # ensure all requested columns exist
    for c in cols:
        if c not in sub.columns:
            sub[c] = ""

    safe_cat = cat.replace("/", "_").replace(" ", "_")
    outp = f"Triage_{safe_cat}_top{topn}.csv"
    sub[cols].head(topn).to_csv(outp, index=False)
    print("Wrote", outp, "rows:", min(len(sub), topn))

# your category set (keep as you had it)
cats = [
    "SSH & Credential Abuse",
    "Cryptomining on HPC",
    "NFS / File–Share Exposure",
    "JupyterHub / Open OnDemand",
]

for c in cats:
    triage(c, 200)


# ------------------------------------------
# Suggested_Selected_master (one union file)
# ------------------------------------------
suggested = []
for c in cats:
    sub = df[df["Category_Guess"] == c].copy()
    if sub.empty:
        continue

    sub = sub.sort_values("Score", ascending=False)

    # "strong" = high-quality rows first (Quality4 >= 2), up to 120;
    # if insufficient, top up with the next best by Score.
    strong = sub[sub["Quality4"] >= 2].head(120)
    if len(strong) < 120:
        strong = pd.concat([strong, sub.head(120)], ignore_index=True)
        strong = strong.drop_duplicates(subset=["URL"])

    if strong.empty:
        continue

    strong["Status"] = "Selected"

    cols = [c for c in (
        "URL","Title","Source_Domain","Category_Guess","Publish_Date","Score",
        "RepFlag","SigFlag","Quality2","Quality4","Status"
    ) if c in strong.columns]

    suggested.append(strong.reindex(columns=cols))

# write union file (or an empty file if nothing to write)
if suggested:
    out_df = pd.concat(suggested, ignore_index=True).drop_duplicates("URL")
    out_df.to_csv("Suggested_Selected_master.csv", index=False)
    print("Wrote Suggested_Selected_master.csv with", len(out_df), "rows")
else:
    open("Suggested_Selected_master.csv", "w").write("")
    print("[WARN] no rows -> Suggested_Selected_master.csv (empty)")


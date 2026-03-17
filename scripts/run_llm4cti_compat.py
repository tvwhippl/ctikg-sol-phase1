#!/usr/bin/env python3
import argparse
import json
import os
import re
from pathlib import Path

import json_repair
import networkx as nx
import pandas as pd
from openai import OpenAI


PROMPT = """You are extracting cyber threat intelligence from one CTI article.

Return exactly two JSON arrays using these markers:

#Final_Entity_List_Start#
json
[{{"entity_name":"...", "entity_type":"..."}}]
#Final_Entity_List_End#

#Final_Relationship_List_Start#
json
[{{"source_entity":"...", "target_entity":"...", "relationship":"..."}}]
#Final_Relationship_List_End#

Rules:
- Only include threat-relevant cybersecurity entities and relationships grounded in the article.
- Keep names concise and normalized.
- Do not invent facts.
- Return only the two marked sections.

Article title: {title}

Article content:
{content}
"""


def extract_marked_json(text, start_marker, end_marker):
    pattern = re.escape(start_marker) + r"\s*json\s*(\[[\s\S]*?\])\s*" + re.escape(end_marker)
    m = re.search(pattern, text)
    if not m:
        return []
    try:
        return json_repair.loads(m.group(1))
    except Exception:
        return []


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--articles-xlsx", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--model", default=os.environ.get("LLM_MODEL", "llama4-scout-17b"))
    ap.add_argument("--api-base", default=os.environ.get("OPENAI_BASE_URL"))
    ap.add_argument("--api-key", default=os.environ.get("OPENAI_API_KEY"))
    ap.add_argument("--max-articles", type=int, default=2)
    ap.add_argument("--max-chars", type=int, default=4000)
    ap.add_argument("--max-tokens", type=int, default=1800)
    ap.add_argument("--temperature", type=float, default=0.0)
    args = ap.parse_args()

    if not args.api_base or not args.api_key:
        raise SystemExit("OPENAI_BASE_URL and OPENAI_API_KEY are required")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_excel(args.articles_xlsx).head(args.max_articles).copy()
    client = OpenAI(api_key=args.api_key, base_url=args.api_base)

    raw_rows = []
    edge_rows = []
    node_rows = []
    G = nx.MultiDiGraph()

    for _, row in df.iterrows():
        article_id = row["ArticleIndex"]
        title = str(row.get("title", "")).strip()
        content = str(row.get("content", "")).strip()[: args.max_chars]

        prompt = PROMPT.format(title=title, content=content)

        resp = client.chat.completions.create(
            model=args.model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=args.max_tokens,
            temperature=args.temperature,
        )
        raw_text = resp.choices[0].message.content or ""

        entities = extract_marked_json(
            raw_text,
            "#Final_Entity_List_Start#",
            "#Final_Entity_List_End#",
        )
        rels = extract_marked_json(
            raw_text,
            "#Final_Relationship_List_Start#",
            "#Final_Relationship_List_End#",
        )

        raw_rows.append({
            "ArticleIndex": article_id,
            "title": title,
            "kg_raw": raw_text,
            "entity_count": len(entities),
            "relationship_count": len(rels),
        })

        for ent in entities:
            name = str(ent.get("entity_name", "")).strip()
            etype = str(ent.get("entity_type", "")).strip()
            if not name:
                continue
            G.add_node(name, entity_type=etype)
            node_rows.append({
                "ArticleIndex": article_id,
                "entity_name": name,
                "entity_type": etype,
            })

        for rel in rels:
            s = str(rel.get("source_entity", "")).strip()
            t = str(rel.get("target_entity", "")).strip()
            r = str(rel.get("relationship", "")).strip()
            if not s or not t or not r:
                continue
            G.add_edge(s, t, relationship=r, ArticleIndex=article_id)
            edge_rows.append({
                "ArticleIndex": article_id,
                "source_entity": s,
                "target_entity": t,
                "relationship": r,
            })

    pd.DataFrame(raw_rows).to_csv(out_dir / "article_kg_raw.csv", index=False)
    pd.DataFrame(node_rows).to_csv(out_dir / "graph_nodes.csv", index=False)
    pd.DataFrame(edge_rows).to_csv(out_dir / "graph_edges.csv", index=False)

    nx.write_gexf(G, out_dir / "graph.gexf")

    summary = {
        "articles_processed": len(raw_rows),
        "nodes": G.number_of_nodes(),
        "edges": G.number_of_edges(),
        "model": args.model,
        "api_base": args.api_base,
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("articles_processed:", summary["articles_processed"])
    print("nodes:", summary["nodes"])
    print("edges:", summary["edges"])
    print("out_dir:", out_dir)


if __name__ == "__main__":
    main()

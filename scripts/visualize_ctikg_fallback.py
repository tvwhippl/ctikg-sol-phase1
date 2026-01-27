#!/usr/bin/env python3
"""
Fallback visualizer that writes a standalone HTML using vis-network CDN.
Usage:
  python scripts/visualize_ctikg_fallback.py --input results/simple_ctikg_results.jsonl --out results/simple_ctikg_viz_fallback.html --top 150
"""
import json, argparse
from collections import Counter, defaultdict

def build_graph(input_path, top_n=150):
    nodes_count = Counter()
    edges_count = Counter()
    edge_meta = defaultdict(list)

    with open(input_path, "r", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip(): continue
            j = json.loads(line)
            res = j.get("result") or {}
            triples = res.get("triples") if isinstance(res, dict) else None
            if not triples or not isinstance(triples, list):
                continue
            for t in triples:
                if not isinstance(t, dict): continue
                s = (t.get("subject") or t.get("s") or "").strip()
                o = (t.get("object") or t.get("o") or "").strip()
                r = (t.get("relation") or t.get("rel") or t.get("predicate") or "(rel)").strip()
                if not s or not o: continue
                nodes_count[s] += 1
                nodes_count[o] += 1
                edges_count[(s,o,r)] += 1
                edge_meta[(s,o)].append(r)

    # pick top nodes by degree
    most = set([n for n,_ in nodes_count.most_common(top_n)])
    nodes = []
    node_ids = {}
    idx = 1
    for n in most:
        node_ids[n] = idx
        nodes.append({"id": idx, "label": n, "value": nodes_count[n]})
        idx += 1

    edges = []
    for (s,o,r),count in edges_count.items():
        if s in most and o in most:
            edges.append({
                "from": node_ids[s],
                "to": node_ids[o],
                "arrows": "to",
                "label": f"{r} ({count})",
                "title": "Relations: " + ", ".join(set(edge_meta[(s,o)]))
            })

    return {"nodes": nodes, "edges": edges}

def write_html(graph, out_path, title="CTIKG Viz Fallback"):
    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>{title}</title>
  <script type="text/javascript" src="https://unpkg.com/vis-network@9.1.2/dist/vis-network.min.js"></script>
  <style>
    body {{ margin: 0; background:#111; color:#eee; }}
    #mynetwork {{ width: 100%; height: 95vh; border: 1px solid #444; }}
    h3 {{ margin:8px 10px; color:#eee; }}
  </style>
</head>
<body>
<h3 style="margin:8px 10px; color:#eee;">{title} — nodes: {len(graph['nodes'])}, edges: {len(graph['edges'])}</h3>
<div id="mynetwork"></div>
<script type="text/javascript">
const nodes = {json.dumps(graph['nodes'])};
const edges = {json.dumps(graph['edges'])};
const container = document.getElementById('mynetwork');
const data = {{ nodes: new vis.DataSet(nodes), edges: new vis.DataSet(edges) }};
const options = {{
  nodes: {{ shape: 'dot', font: {{ color: '#fff' }}, scaling: {{ min:6, max:60 }} }},
  edges: {{ color: 'rgba(200,200,200,0.3)', smooth:false, font: {{ color: '#fff' }} }},
  physics: {{ stabilization: false, barnesHut: {{ gravitationalConstant: -20000 }} }},
  interaction: {{ hover: true, tooltipDelay: 100 }}
}};
const network = new vis.Network(container, data, options);
</script>
</body>
</html>
"""
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write(html)
    print("Wrote", out_path)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="results/simple_ctikg_results.jsonl")
    p.add_argument("--out", default="results/simple_ctikg_viz_fallback.html")
    p.add_argument("--top", type=int, default=150)
    args = p.parse_args()
    g = build_graph(args.input, top_n=args.top)
    write_html(g, args.out)

#!/usr/bin/env python3
"""
Simple pyvis visualization for Simple CTIKG results.
Usage:
  pip install pyvis networkx
  python scripts/visualize_ctikg.py --input results/simple_ctikg_results.jsonl --out results/simple_ctikg_viz.html
"""
import argparse, json
from pyvis.network import Network
from collections import Counter, defaultdict

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="results/simple_ctikg_results.jsonl")
    p.add_argument("--out", default="results/simple_ctikg_viz.html")
    args = p.parse_args()

    nodes = Counter()
    edges = Counter()
    edge_meta = defaultdict(list)

    with open(args.input, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line: continue
            obj = json.loads(line)
            res = obj.get("result") or {}
            triples = res.get("triples") if isinstance(res, dict) else None
            if not triples: 
                continue
            for t in triples:
                # skip malformed triples
                if not isinstance(t, dict):
                    continue

                s = t.get("subject") or t.get("s") or "(unknown-subject)"
                r = t.get("relation") or t.get("rel") or t.get("predicate") or "(rel)"
                o = t.get("object") or t.get("o") or "(unknown-object)"

                if not s or not o:
                    continue


    # build pyvis network
    net = Network(height="1200px", width="100%", bgcolor="#111111", font_color="#eeeeee", notebook=False)
    # scale node sizes
    maxdeg = max(nodes.values()) if nodes else 1
    for n,deg in nodes.items():
        size = 10 + (deg/maxdeg)*40
        net.add_node(n, label=n, title=f"{n}\ndegree: {deg}", value=deg, size=size)

    for (s,o,r),count in edges.items():
        label = f"{r} ({count})"
        title = "Relations: " + ", ".join(set(edge_meta[(s,o)]))
        net.add_edge(s, o, title=title, label=label, value=count, physics=True)

    net.set_options("""
    var options = {
      "nodes": {"font":{"size":14}},
      "edges": {"color": {"inherit": true}, "smooth": false},
      "physics": {"barnesHut": {"gravitationalConstant": -20000, "springLength": 95}}
    }
    """)
    net.show(args.out)
    print("Wrote vis to", args.out)

if __name__ == "__main__":
    main()

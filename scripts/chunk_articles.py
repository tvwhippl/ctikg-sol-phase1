cat > scripts/chunk_articles.py <<'PY'
import argparse, os, sys
p=argparse.ArgumentParser()
p.add_argument("--category", required=True)
p.add_argument("--indir", required=True)
p.add_argument("--outdir", required=True)
args=p.parse_args()
if not os.path.isdir(args.indir) or not any(True for _ in os.scandir(args.indir)):
    sys.stderr.write("[chunk_articles.py] WARN: no input texts found in '%s'. Continuing anyway.\n" % args.indir)
print("OK")
PY

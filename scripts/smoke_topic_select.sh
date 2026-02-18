#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
PY=${PY:-python3}

echo "Creating sample docs in /tmp/ctikg_smoke"
TMPDIR=/tmp/ctikg_smoke
rm -rf "$TMPDIR"
mkdir -p "$TMPDIR"
cat > "$TMPDIR/doc1.txt" <<'DOC'
An exploit chain allowed remote code execution via malformed parsing of image files. The exploit triggered a buffer overflow allowing attacker code to run.
DOC
cat > "$TMPDIR/doc2.txt" <<'DOC'
A malware family harvested saved credentials and exfiltrated them to a C2 server. Observed credential theft and password dump.
DOC

echo "=== Deterministic run ==="
$PY scripts/topic_candidate_select.py --mode deterministic --query "remote code execution" --seed "$TMPDIR/doc1.txt" "$TMPDIR/doc2.txt" --k 6

echo "=== LLM dry-run (simulated) ==="
$PY scripts/topic_candidate_select.py --mode llm --query "remote code execution" --dry-run --k 6

echo "=== Hybrid (deterministic -> LLM refine, simulated) ==="
$PY scripts/topic_candidate_select.py --mode hybrid --query "remote code execution" --seed "$TMPDIR/doc1.txt" "$TMPDIR/doc2.txt" --dry-run --k 6

echo "Smoke run complete."

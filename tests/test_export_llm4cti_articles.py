from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd


def test_export_llm4cti_articles_dedup_and_min_chars(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs" / "Remote_Code_Execution" / "20260101-000000"
    scrape_dir = run_dir / "scrape"
    scrape_dir.mkdir(parents=True, exist_ok=True)

    records = [
        {
            "url": "https://example.com/a",
            "title": "Doc A",
            "text": "alpha " * 80,
            "source_domain": "example.com",
            "category": "Remote Code Execution",
        },
        {
            "url": "https://example.com/a",
            "title": "Doc A duplicate",
            "text": "beta " * 90,
            "source_domain": "example.com",
            "category": "Remote Code Execution",
        },
        {
            "url": "https://example.com/b",
            "title": "Too Short",
            "text": "short text",
            "source_domain": "example.com",
            "category": "Remote Code Execution",
        },
        {
            "url": "https://example.com/c",
            "title": "Doc C",
            "content": "gamma " * 70,
            "source_domain": "example.com",
            "category": "Remote Code Execution",
        },
    ]

    in_path = scrape_dir / "scraped_corpus.jsonl"
    with in_path.open("w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")

    script = Path(__file__).resolve().parents[1] / "scripts" / "export_llm4cti_articles.py"
    subprocess.check_call(
        [
            sys.executable,
            str(script),
            "--run-dir",
            str(run_dir),
            "--min-chars",
            "200",
        ],
        cwd=Path(__file__).resolve().parents[1],
    )

    out_dir = run_dir / "llm4cti"
    csv_path = out_dir / "llm4cti_articles.csv"
    xlsx_path = out_dir / "Articles.xlsx"
    meta_path = out_dir / "llm4cti_articles_meta.json"

    assert csv_path.is_file()
    assert xlsx_path.is_file()
    assert meta_path.is_file()

    df = pd.read_csv(csv_path)
    assert len(df) == 2
    assert df["ArticleIndex"].tolist() == [1, 2]
    assert df["url"].tolist() == ["https://example.com/a", "https://example.com/c"]

    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    assert meta["docs"] == 2
    assert "content" in meta["columns"]

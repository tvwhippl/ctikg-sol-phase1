from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts import category_select


def test_open_topic_category_select_writes_selected_csv(tmp_path: Path) -> None:
    # Minimal queue with two rows
    q = pd.DataFrame(
        {
            "URL": ["https://example.com/rce", "https://example.com/other"],
            "Title": ["Remote code execution in Redis", "Unrelated news"],
            "Snippet": ["Exploit allows RCE", "Nothing to see"],
            "Source_Domain": ["example.com", "example.com"],
            "Quality4": [0.9, 0.1],
            "Quality2": [0.8, 0.1],
            "RepFlag": [1, 0],
            "SigFlag": [1, 0],
            "Score": [0.75, 0.05],
        }
    )
    in_csv = tmp_path / "queue.csv"
    q.to_csv(in_csv, index=False)

    cat_yaml = tmp_path / "cat.yaml"
    cat_yaml.write_text(
        """name: Remote Code Execution
include:
  - remote code execution
  - rce
exclude:
  - sports
winners: 10
""",
        encoding="utf-8",
    )

    out_csv = tmp_path / "Selected_Remote_Code_Execution.csv"
    out_path = category_select.run(str(in_csv), str(cat_yaml), out_path=str(out_csv))

    assert Path(out_path).exists()
    out = pd.read_csv(out_path)
    assert len(out) >= 1
    assert set(["URL", "Title", "Source_Domain", "Category"]).issubset(out.columns)
    assert out.iloc[0]["Category"] == "Remote Code Execution"

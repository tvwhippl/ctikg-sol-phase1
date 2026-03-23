from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import category_select


def test_open_topic_category_select_writes_selected_csv_and_summary(tmp_path: Path) -> None:
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
    summary_json = tmp_path / "selection_summary.json"

    out_path = category_select.run(
        str(in_csv),
        str(cat_yaml),
        out_path=str(out_csv),
        selection_summary_path=str(summary_json),
    )

    assert Path(out_path).exists()
    out = pd.read_csv(out_path)
    assert len(out) >= 1
    assert set(["URL", "Title", "Source_Domain", "Category"]).issubset(out.columns)
    assert out.iloc[0]["Category"] == "Remote Code Execution"

    summary = json.loads(summary_json.read_text(encoding="utf-8"))
    assert summary["selected_rows"] >= 1
    assert summary["strict_candidate_count"] >= 1
    assert summary["stop_reason"] in {"filled_from_strict", "underfilled_after_strict_topic_gate"}


def test_anchor_gate_min_hits_filters_single_anchor_match(tmp_path: Path) -> None:
    q = pd.DataFrame(
        {
            "URL": [
                "https://example.com/openssh",
                "https://example.com/private-key",
            ],
            "Title": [
                "OpenSSH vulnerabilities",
                "python-cryptography vulnerability",
            ],
            "Snippet": [
                "OpenSSH incorrectly handled NULL characters in ssh:// URIs and ProxyCommand usernames.",
                "A remote attacker could recover the least significant bits of private keys.",
            ],
            "Source_Domain": ["example.com", "example.com"],
            "Quality4": [1.0, 0.8],
            "Quality2": [1.0, 0.8],
            "RepFlag": [1, 1],
            "SigFlag": [1, 0],
            "Score": [0.9, 0.7],
        }
    )
    in_csv = tmp_path / "queue.csv"
    q.to_csv(in_csv, index=False)

    cat_yaml = tmp_path / "cat.yaml"
    cat_yaml.write_text(
        """name: SSH Credential Abuse and Lateral Movement
include:
  - SSH credential abuse
  - Lateral movement
  - Private key compromise
exclude:
  - sports
fallback_anchors:
  - ssh
  - openssh
  - private key
fallback_anchor_min_hits: 2
winners: 10
""",
        encoding="utf-8",
    )

    out_csv = tmp_path / "Selected_SSH.csv"
    summary_json = tmp_path / "selection_summary.json"

    out_path = category_select.run(
        str(in_csv),
        str(cat_yaml),
        out_path=str(out_csv),
        selection_summary_path=str(summary_json),
    )

    out = pd.read_csv(out_path)
    assert len(out) == 1
    assert out.iloc[0]["Title"] == "OpenSSH vulnerabilities"

    summary = json.loads(summary_json.read_text(encoding="utf-8"))
    assert summary["strict_candidate_count"] == 0
    assert summary["qsim_base_candidate_count"] >= 2
    assert summary["anchor_gate_candidate_count"] == 1
    assert summary["qsim_candidate_count"] == 1
    assert summary["qsim_rejected_by_anchor_count"] >= 1
    assert summary["stop_reason"] == "underfilled_after_anchor_gate"

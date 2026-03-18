from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

from scripts import run_llm4cti_compat as compat


class _DummyMessage:
    def __init__(self, content: str) -> None:
        self.content = content


class _DummyChoice:
    def __init__(self, content: str) -> None:
        self.message = _DummyMessage(content)


class _DummyResponse:
    def __init__(self, content: str) -> None:
        self.choices = [_DummyChoice(content)]


class _DummyCompletions:
    def __init__(self, outputs: list[str]) -> None:
        self._outputs = outputs
        self._i = 0

    def create(self, **kwargs):
        out = self._outputs[self._i]
        self._i += 1
        return _DummyResponse(out)


class _DummyChat:
    def __init__(self, outputs: list[str]) -> None:
        self.completions = _DummyCompletions(outputs)


class _DummyClient:
    def __init__(self, outputs: list[str]) -> None:
        self.chat = _DummyChat(outputs)


def test_extract_marked_json_and_main(monkeypatch, tmp_path: Path) -> None:
    articles = pd.DataFrame(
        [
            {
                "ArticleIndex": 1,
                "title": "Doc One",
                "url": "https://example.com/1",
                "source_domain": "example.com",
                "category": "Remote Code Execution",
                "content": "Alpha article content",
                "content_chars": 1000,
                "text_sha256": "a",
            },
            {
                "ArticleIndex": 2,
                "title": "Doc Two",
                "url": "https://example.com/2",
                "source_domain": "example.com",
                "category": "Remote Code Execution",
                "content": "Beta article content",
                "content_chars": 900,
                "text_sha256": "b",
            },
        ]
    )

    xlsx = tmp_path / "Articles.xlsx"
    out_dir = tmp_path / "compat_out"
    articles.to_excel(xlsx, index=False)

    outputs = [
        """#Final_Entity_List_Start#
json
[{"entity_name":"HPE","entity_type":"Organization"},{"entity_name":"AOS-CX","entity_type":"Software"}]
#Final_Entity_List_End#

#Final_Relationship_List_Start#
json
[{"source_entity":"HPE","target_entity":"AOS-CX","relationship":"Develops"}]
#Final_Relationship_List_End#""",
        """#Final_Entity_List_Start#
json
[{"entity_name":"GIMP","entity_type":"Software"}]
#Final_Entity_List_End#

#Final_Relationship_List_Start#
json
[]
#Final_Relationship_List_End#""",
    ]

    monkeypatch.setattr(compat, "OpenAI", lambda **kwargs: _DummyClient(outputs))

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_llm4cti_compat.py",
            "--articles-xlsx",
            str(xlsx),
            "--out-dir",
            str(out_dir),
            "--api-base",
            "https://example.invalid/v1",
            "--api-key",
            "sk-test",
            "--model",
            "dummy-model",
        ],
    )

    compat.main()

    summary = json.loads((out_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["articles_processed"] == 2
    assert summary["nodes"] == 3
    assert summary["edges"] == 1

    nodes = pd.read_csv(out_dir / "graph_nodes.csv")
    edges = pd.read_csv(out_dir / "graph_edges.csv")

    assert sorted(nodes["entity_name"].tolist()) == ["AOS-CX", "GIMP", "HPE"]
    assert len(edges) == 1
    assert edges.iloc[0]["relationship"] == "Develops"

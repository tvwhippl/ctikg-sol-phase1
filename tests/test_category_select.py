import json
import os
import tempfile
from scripts.topic_candidate_select import select_topics
from scripts import gen_category_from_llm as llm_mod

def test_deterministic_consistent_output(tmp_path):
    # small corpus: two documents
    docs = [str(tmp_path / "a.txt"), str(tmp_path / "b.txt")]
    (tmp_path / "a.txt").write_text("remote code execution exploit buffer overflow vuln")
    (tmp_path / "b.txt").write_text("credential theft password dump exfiltrate")
    res = select_topics("rce password", seed_article_paths_or_ids=docs, runtime_mode="deterministic", out_path=None, k_det=4)
    assert isinstance(res, list)
    # deterministic should return up to k items and canonical ids deterministic
    ids = [r["canonical_id"] for r in res]
    assert len(ids) > 0
    # running again should produce same IDs
    res2 = select_topics("rce password", seed_article_paths_or_ids=docs, runtime_mode="deterministic", out_path=None, k_det=4)
    ids2 = [r["canonical_id"] for r in res2]
    assert ids == ids2

def test_llm_dry_run_returns_canned(monkeypatch):
    monkeypatch.setattr(llm_mod, "CANNED_RESPONSE", llm_mod.CANNED_RESPONSE)
    res = select_topics("any query", seed_article_paths_or_ids=[], runtime_mode="llm", dry_run=True)
    assert isinstance(res, list)
    assert any("Remote Code Execution" in r["label"] or "Credential Theft" in r["label"] for r in res)

def test_hybrid_merges_duplicates(monkeypatch):
    # stub LLMClient to echo a merging result that collapses duplicates
    class StubClient:
        def __init__(self, dry_run=False):
            self.dry_run = dry_run
        def call_llm_for_categories(self, prompt, **kwargs):
            # pretend LLM merged two near-duplicates into one
            return [
                {"label":"Remote Code Execution", "canonical_id":"rce-xxxx", "short_description":"desc", "confidence_score":0.9, "examples":["doc1"]},
                {"label":"Credential Theft", "canonical_id":"cred-xxxx", "short_description":"desc", "confidence_score":0.8, "examples":["doc2"]}
            ]
    monkeypatch.setattr("scripts.topic_candidate_select.LLMClient", lambda dry_run=False: StubClient(dry_run=dry_run))
    docs = []
    res = select_topics("rce credential", seed_article_paths_or_ids=docs, runtime_mode="hybrid", dry_run=False)
    labels = [r["label"].lower() for r in res]
    assert "remote code execution" in labels
    assert "credential theft" in labels


def test_validate_topic_config_preserves_fallback_anchor_fields():
    cfg = llm_mod._validate_topic_config(
        {
            "name": "SSH Credential Abuse and Lateral Movement",
            "include": ["SSH credential abuse", "Private key compromise"],
            "exclude": ["sports"],
            "fallback_anchors": ["ssh", "openssh"],
            "fallback_anchor_min_hits": 2,
            "winners": 50,
        },
        default_name="SSH Credential Abuse and Lateral Movement",
        default_winners=50,
    )
    assert cfg["fallback_anchors"] == ["ssh", "openssh"]
    assert cfg["fallback_anchor_min_hits"] == 2

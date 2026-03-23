# Writing effective topics (for selection & scraping)

The goal is to concentrate the link queue on **a narrow theme** so scraping stays relevant and the selector can stop cleanly when the remaining pool turns into junk.

## Recommended YAML shape

```yaml
name: SSH Credential Abuse and Lateral Movement
include:
  - SSH credential abuse
  - SSH brute force
  - SSH lateral movement
  - Private key compromise
  - Lateral movement detection
exclude:
  - web application vulnerability
  - malware outbreak investigation
  - failed login attempt
fallback_anchors:
  - ssh
  - openssh
  - lateral movement
  - private key
fallback_anchor_min_hits: 2
winners: 50
```

## What each field does

- `include`
  - the strict topic gate
  - if candidates clearly match these phrases and avoid excludes, they rank as true topic matches

- `exclude`
  - removes obviously wrong classes of content

- `fallback_anchors`
  - optional but recommended for quality-sensitive topics
  - used only when strict matching is empty and the selector falls back to semantic similarity
  - these should be **high-precision substrings or short phrases** such as protocols, products, services, or compound terms

- `fallback_anchor_min_hits`
  - how many fallback anchors a semantic-fallback candidate must hit before it is admitted
  - use `1` when anchors are already very specific
  - use `2` when a single anchor would still allow broad or misleading content

- `winners`
  - the desired downstream cap
  - underfill is acceptable when the quality gate stops the ranking early

## Patterns that work well

- prefer protocol, product, service, or compound anchors over generic words
- good anchors:
  - `ssh`
  - `openssh`
  - `lateral movement`
  - `private key`
- weaker anchors that often need pairing or should be avoided alone:
  - `credential`
  - `vulnerability`
  - `attack`
  - `exploit`

## Using the LLM helper

- Set environment:
  ```bash
  export LLM_PROVIDER=ollama
  export LLM_MODEL=llama3.1:8b
  export LLM_BASE_URL="http://127.0.0.1:11434"
  ```
- Generate:
  ```bash
  make topic-gen TOPIC="CI/CD pipeline attacks: runner poisoning, OIDC misconfiguration, artifact/cache poisoning"
  ```
- The generated YAML is editable. Review and tighten:
  - `include`
  - `exclude`
  - `fallback_anchors`
  - `fallback_anchor_min_hits`

## Quality bar

After selection, inspect the selection summary JSON written next to the selected CSV.

Useful signals:
- `strict_candidate_count`
- `qsim_base_candidate_count`
- `anchor_gate_candidate_count`
- `qsim_rejected_by_anchor_count`
- `stop_reason`

Typical healthy outcomes:
- `filled_from_strict`
- `filled_from_qsim_anchor_fallback`
- `underfilled_after_anchor_gate`

If you see:
- `no_candidates_passing_anchor_gate`
- a very large `qsim_rejected_by_anchor_count`

that usually means the topic is too broad, the anchors are too weak, or the available source pool does not support the topic cleanly at that scale.

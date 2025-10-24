# Writing effective topics (for selection & scraping)

The goal is to concentrate the link queue on **a narrow theme** so scraping stays fast and relevant.

## Patterns that work well

- **Concise label + bullet inclusions**

```yaml
name: CI/CD Pipeline Attacks
include:
  - runner poisoning
  - OIDC misconfiguration
  - artifact or cache poisoning
  - secrets exfiltration via CI logs
exclude:
  - tutorial
  - getting started
  - press release
winners: 100
```

- Keep the **include list specific** (attack names, misconfig types, concrete techniques).
- Use **exclude** to eliminate meta‑content (tutorials, job postings, releases).

## Using the LLM helper (Ollama)

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
- The generated YAML is editable. Open it under `configs/categories/_generated/` and adjust `include/exclude/winners`.

## Quality bar

- After `topic-pull`, run:
  ```bash
  python scripts/make_helper_flags.py
  head -n 3 data/Links_Queue_sorted_flags.csv
  ```
- Skim the per‑category packs: `Triage_*_top200.csv`.
- Iterate on the YAML if you see off‑topic drift or too many news/press items.


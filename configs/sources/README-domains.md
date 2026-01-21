Example domain-based sources for pre_rank_links_v3.py and topic pipeline.

Required shape:
{
  "domains": {
    "example.com": {
      "weight": 0.8,
      "rss": ["https://example.com/feed"],
      "indexes": [{"url":"https://example.com/archive","link_pattern":"..."}]
    },
    ...
  }
}

Notes:
- pre_rank_links_v3.py expects top-level 'domains'. If absent, it will produce zero rows.
- For search-based sources you must supply seed_urls or API credentials (SERPAPI_KEY / GOOGLE_API_KEY) — do not commit secrets.
- Use configs/sources/common_domains.json for local testing only; production sources should be managed separately.

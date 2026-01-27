
## OpenRouter provider notes (tested)

- Set `OPENROUTER_API_KEY` in the environment before running.
- Use `--provider openrouter` to select OpenRouter.
- Always run with `--max-docs 10` first and inspect results.
- Use `--dry-run` to preview prompts without cost.
- Use `--throttle-ms 250` or higher to be conservative.
- Any run >25 docs requires `--confirm` flag.

- Verified Ollama test: Tue Jan 27 15:11:09 -05 2026 model: llama3.2:latest

### OpenRouter (validated)

- Endpoint: https://openrouter.ai/api/v1/chat/completions
- Tested model: gpt-4o-mini
- Avg cost per doc (Simple CTIKG): ~$0.00005
- Estimated cost for 1,700 docs: <$0.10
- Runner automatically maps local Ollama model names to a safe OpenRouter test model.
- LLM output is sanitized to remove markdown/code fences before JSON parsing.


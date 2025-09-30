# Python
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# LLM (Ollama, local)
export LLM_PROVIDER=ollama
export LLM_MODEL=llama3.1
ollama serve  # (in another terminal) 
ollama pull llama3.1

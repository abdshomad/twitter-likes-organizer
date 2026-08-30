# AI Tagging & Vector Embeddings

- **Built**: Autonomous context-aware tagging engine via local Ollama LLM / OpenRouter fallback and FastEmbed `BAAI/bge-m3` dense vector generator.
- **Paths**: `src/ai/tagger.py`, `src/ai/embedder.py`, `packages/plugin-ai/`
- **Usage**: `AITagger().generate_tags(text)` / `VectorEmbedder().embed_text(text)`

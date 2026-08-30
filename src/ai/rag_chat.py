import json
from typing import Any, AsyncGenerator
import httpx
from src.storage.lancedb_client import LanceDBStore
from src.ai.embedder import VectorEmbedder

OLLAMA_API_URL = "http://127.0.0.1:11434/api/generate"
DEFAULT_MODEL = "gemma4:latest"


class RAGChatEngine:
    def __init__(self, store: LanceDBStore, embedder: VectorEmbedder):
        self.store = store
        self.embedder = embedder
        self.client = httpx.AsyncClient(timeout=60.0)

    async def get_available_model(self) -> str:
        try:
            res = await self.client.get("http://127.0.0.1:11434/api/tags", timeout=2.0)
            if res.status_code == 200:
                models = [m["name"] for m in res.json().get("models", [])]
                for preferred in ["gemma4:latest", "qwen3.6:latest", "qwen3-coder:30b", "test:latest"]:
                    if preferred in models:
                        return preferred
                if models:
                    return models[0]
        except Exception:
            pass
        return DEFAULT_MODEL

    def retrieve_relevant_likes(self, query: str, top_k: int = 6) -> list[dict[str, Any]]:
        vector = self.embedder.embed_text(query)
        results = self.store.search_hybrid(
            query=query,
            query_vector=vector,
            limit=top_k,
        )
        return results

    async def stream_chat_response(
        self,
        query: str,
        conversation_history: list[dict[str, str]] | None = None,
    ) -> AsyncGenerator[str, None]:
        relevant = self.retrieve_relevant_likes(query, top_k=6)
        
        # Send retrieval metadata first as JSON event
        context_sources = []
        context_text_blocks = []
        for idx, tweet in enumerate(relevant, 1):
            handle = tweet.get("author_handle") or "user"
            author = tweet.get("author_name") or handle
            text = tweet.get("text", "").replace("\n", " ")
            url = tweet.get("url") or f"https://x.com/{handle}/status/{tweet.get('tweet_id')}"
            context_sources.append({
                "index": idx,
                "tweet_id": tweet.get("tweet_id"),
                "author_name": author,
                "author_handle": handle,
                "text": text,
                "url": url,
            })
            context_text_blocks.append(f"[{idx}] Author: @{handle} ({author})\nURL: {url}\nContent: {text}\n")

        yield f"data: {json.dumps({'type': 'sources', 'sources': context_sources})}\n\n"

        context_str = "\n".join(context_text_blocks)
        system_prompt = (
            "You are an intelligent AI Assistant with direct RAG access to the user's saved Twitter/X Likes in LanceDB.\n"
            "Answer the user's inquiry accurately, insightfully, and concisely based strictly on the retrieved likes below.\n"
            "Always cite your sources using bracket notation like [1], [2] when referring to specific tweets.\n\n"
            f"=== RETRIEVED LIKES FROM LANCEDB ===\n{context_str}\n"
        )

        model = await self.get_available_model()
        payload = {
            "model": model,
            "prompt": f"{system_prompt}\nUser Query: {query}\n\nAssistant Response:",
            "stream": True,
            "options": {"temperature": 0.3, "top_p": 0.9},
        }

        try:
            async with self.client.stream("POST", OLLAMA_API_URL, json=payload) as response:
                if response.status_code == 200:
                    async for chunk in response.aiter_lines():
                        if chunk:
                            data = json.loads(chunk)
                            token = data.get("response", "")
                            if token:
                                yield f"data: {json.dumps({'type': 'token', 'token': token})}\n\n"
                            if data.get("done"):
                                break
                else:
                    # Fallback synthesis
                    yield f"data: {json.dumps({'type': 'token', 'token': f'Found {len(relevant)} matching likes in your LanceDB database. Summaries below:'})}\n\n"
        except Exception:
            # Local fallback summary if Ollama connection fails
            summary = "\n\n".join([f"• **@{s['author_handle']}**: {s['text']} ([View Tweet]({s['url']}))" for s in context_sources])
            yield f"data: {json.dumps({'type': 'token', 'token': f'Retrieved {len(relevant)} relevant likes from LanceDB:\n\n{summary}'})}\n\n"

        yield f"data: {json.dumps({'type': 'done'})}\n\n"

    async def close(self):
        await self.client.aclose()

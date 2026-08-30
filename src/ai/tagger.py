import json
import os
import re
from typing import Any
import httpx

DEFAULT_OLLAMA_URL = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434")
DEFAULT_OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")


class AITagger:
    def __init__(
        self,
        ollama_url: str | None = None,
        model: str | None = None,
        openrouter_key: str | None = None,
    ):
        self.ollama_url = ollama_url or DEFAULT_OLLAMA_URL
        self.model = model or DEFAULT_OLLAMA_MODEL
        self.openrouter_key = openrouter_key or OPENROUTER_API_KEY

    def generate_tags(self, tweet_text: str) -> list[str]:
        if not tweet_text.strip():
            return []

        # 1. Try local Ollama
        tags = self._tag_with_ollama(tweet_text)
        if tags:
            return tags

        # 2. Try OpenRouter fallback
        if self.openrouter_key:
            tags = self._tag_with_openrouter(tweet_text)
            if tags:
                return tags

        # 3. Rule-based heuristic extraction fallback
        return self._heuristic_tags(tweet_text)

    def _tag_with_ollama(self, tweet_text: str) -> list[str]:
        prompt = (
            "Analyze the following tweet text and output 3 to 5 concise categorical topic tags as a JSON list of strings.\n"
            "Only output the JSON array (e.g. [\"AI\", \"Machine Learning\", \"Python\"]).\n\n"
            f"Tweet:\n{tweet_text}\n\nJSON Tags:"
        )
        try:
            with httpx.Client(timeout=10.0) as client:
                res = client.post(
                    f"{self.ollama_url}/api/generate",
                    json={"model": self.model, "prompt": prompt, "stream": False, "format": "json"},
                )
                if res.status_code == 200:
                    response_text = res.json().get("response", "")
                    parsed = json.loads(response_text)
                    if isinstance(parsed, list):
                        return [str(t).strip() for t in parsed if t]
                    elif isinstance(parsed, dict) and "tags" in parsed:
                        return [str(t).strip() for t in parsed["tags"] if t]
        except Exception:
            pass
        return []

    def _tag_with_openrouter(self, tweet_text: str) -> list[str]:
        headers = {
            "Authorization": f"Bearer {self.openrouter_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": "meta-llama/llama-3.2-3b-instruct:free",
            "messages": [
                {
                    "role": "user",
                    "content": f"Extract 3-5 concise topic tags for this tweet as JSON array:\n{tweet_text}",
                }
            ],
            "response_format": {"type": "json_object"},
        }
        try:
            with httpx.Client(timeout=15.0) as client:
                res = client.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=payload)
                if res.status_code == 200:
                    content = res.json()["choices"][0]["message"]["content"]
                    parsed = json.loads(content)
                    if isinstance(parsed, list):
                        return [str(t).strip() for t in parsed if t]
                    if isinstance(parsed, dict):
                        for val in parsed.values():
                            if isinstance(val, list):
                                return [str(t).strip() for t in val if t]
        except Exception:
            pass
        return []

    def _heuristic_tags(self, text: str) -> list[str]:
        """Extract hashtags and tech keywords if LLM is offline."""
        hashtags = [re.sub(r"[^\w]", "", h) for h in re.findall(r"#\w+", text)]
        keywords = ["AI", "LLM", "Rust", "Python", "TypeScript", "React", "Linux", "Docker", "Database"]
        found: set[str] = set(hashtags)
        lower_text = text.lower()
        for kw in keywords:
            if kw.lower() in lower_text:
                found.add(kw)
        return list(found)[:5]

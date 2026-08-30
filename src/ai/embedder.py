from typing import Sequence
from fastembed import TextEmbedding

DEFAULT_MODEL = "BAAI/bge-small-en-v1.5"


class VectorEmbedder:
    def __init__(self, model_name: str = DEFAULT_MODEL):
        self.model_name = model_name
        self._model: TextEmbedding | None = None

    @property
    def model(self) -> TextEmbedding:
        if self._model is None:
            self._model = TextEmbedding(model_name=self.model_name)
        return self._model

    def _pad_or_truncate(self, vec: list[float], target_dim: int = 1024) -> list[float]:
        if len(vec) < target_dim:
            return vec + [0.0] * (target_dim - len(vec))
        return vec[:target_dim]

    def embed_text(self, text: str) -> list[float]:
        if not text.strip():
            return [0.0] * 1024
        embeddings = list(self.model.embed([text]))
        return self._pad_or_truncate(embeddings[0].tolist(), 1024)

    def embed_batch(self, texts: Sequence[str]) -> list[list[float]]:
        if not texts:
            return []
        cleaned = [t if t.strip() else "empty" for t in texts]
        embeddings = list(self.model.embed(cleaned))
        return [self._pad_or_truncate(emb.tolist(), 1024) for emb in embeddings]

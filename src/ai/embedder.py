from typing import Sequence
from fastembed import TextEmbedding

DEFAULT_MODEL = "BAAI/bge-m3"


class VectorEmbedder:
    def __init__(self, model_name: str = DEFAULT_MODEL):
        self.model_name = model_name
        self._model: TextEmbedding | None = None

    @property
    def model(self) -> TextEmbedding:
        if self._model is None:
            self._model = TextEmbedding(model_name=self.model_name)
        return self._model

    def embed_text(self, text: str) -> list[float]:
        if not text.strip():
            return [0.0] * 1024
        embeddings = list(self.model.embed([text]))
        return embeddings[0].tolist()

    def embed_batch(self, texts: Sequence[str]) -> list[list[float]]:
        if not texts:
            return []
        cleaned = [t if t.strip() else "empty" for t in texts]
        embeddings = list(self.model.embed(cleaned))
        return [emb.tolist() for emb in embeddings]

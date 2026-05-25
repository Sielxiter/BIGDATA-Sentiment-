"""Central sentiment service for the rest of the project."""

from __future__ import annotations

from config.settings import settings
from nlp.bert_sentiment_model import BertSentimentModel
from nlp.trained_bert_model import TrainedBertSentimentModel


class SentimentService:
    """Stable entry point that hides the concrete sentiment model implementation."""

    def __init__(self):
        self.provider = settings.model_provider.strip().lower()
        self.model_name = settings.model_name
        self.model_version = settings.model_version
        self.device = settings.model_device
        self._model = None

    def _load_model(self):
        if self._model is not None:
            return self._model

        if self.provider == "huggingface":
            self._model = BertSentimentModel(
                model_name=self.model_name,
                model_version=self.model_version,
                device=self.device,
            )
        elif self.provider == "trained":
            self._model = TrainedBertSentimentModel(
                model_name=self.model_name,
                model_version=self.model_version,
                device=self.device,
            )
        else:
            raise ValueError(
                f"Unsupported MODEL_PROVIDER={self.provider!r}. "
                "Use 'huggingface' or 'trained'."
            )

        return self._model

    def predict(self, text: str) -> dict:
        """Predict sentiment for one text using the configured model."""

        return self._load_model().predict(text)

    def predict_batch(self, texts: list[str]) -> list[dict]:
        """Predict sentiment for multiple texts using the configured model."""

        return self._load_model().predict_batch(texts)

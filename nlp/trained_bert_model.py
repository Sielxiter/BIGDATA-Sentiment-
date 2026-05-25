"""Placeholder for the future fine-tuned BERT model."""

from __future__ import annotations

from nlp.base_model import BaseSentimentModel


class TrainedBertSentimentModel(BaseSentimentModel):
    """Reserved for a future fine-tuned model stored in `models/fine_tuned_bert/`."""

    def __init__(self, model_name: str | None = None, model_version: str = "v1", device: str = "cpu"):
        self.model_name = model_name or "models/fine_tuned_bert/"
        self.model_version = model_version
        self.device = device

    def predict(self, text: str) -> dict:
        raise NotImplementedError(
            "TrainedBertSentimentModel is reserved for the future fine-tuned model in "
            "`models/fine_tuned_bert/`. Train and wire it in a later step."
        )

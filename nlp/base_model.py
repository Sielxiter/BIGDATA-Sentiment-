"""Base sentiment model interface.

The rest of the platform should depend on this contract, not on any specific
pretrained or fine-tuned implementation.
"""

from __future__ import annotations

from abc import ABC, abstractmethod


class BaseSentimentModel(ABC):
    """Common interface for replaceable sentiment models."""

    @abstractmethod
    def predict(self, text: str) -> dict:
        raise NotImplementedError

    def predict_batch(self, texts: list[str]) -> list[dict]:
        return [self.predict(text) for text in texts]

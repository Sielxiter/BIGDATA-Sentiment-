"""Pretrained Hugging Face sentiment model implementation."""

from __future__ import annotations

from typing import Any

from nlp.base_model import BaseSentimentModel
from nlp.preprocessing import clean_text

try:
    import torch
    from transformers import AutoModelForSequenceClassification, AutoTokenizer
except ImportError:  # pragma: no cover - handled at runtime with a clear error
    torch = None
    AutoModelForSequenceClassification = None
    AutoTokenizer = None


class BertSentimentModel(BaseSentimentModel):
    """Wrap a pretrained Hugging Face sentiment classifier behind a stable API."""

    def __init__(self, model_name: str, model_version: str = "v1", device: str = "cpu"):
        self.model_name = model_name
        self.model_version = model_version
        self.device = (device or "cpu").strip().lower()
        self._tokenizer = None
        self._model = None
        self._torch_device = self._resolve_torch_device()
        # Keep the first working CPU version predictable on this environment.
        # We can raise this later if we move to a lighter or optimized runtime.
        self._max_length = 32

        # Keep CPU inference predictable on this first version.
        if torch is not None:
            try:
                torch.set_num_threads(1)
                torch.set_num_interop_threads(1)
            except Exception:
                # If the runtime rejects thread tuning, keep going with defaults.
                pass

    def _resolve_torch_device(self):
        if torch is None:
            return None

        if self.device.startswith("cuda") and torch.cuda.is_available():
            return torch.device("cuda")

        if self.device.startswith("cuda"):
            print("[BertSentimentModel] CUDA requested but not available. Falling back to CPU.")

        return torch.device("cpu")

    def _load_model(self):
        if AutoTokenizer is None or AutoModelForSequenceClassification is None:
            raise RuntimeError(
                "transformers/torch are not installed. Run `pip install -r requirements.txt`."
            )

        if self._tokenizer is None or self._model is None:
            self._tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            self._model = AutoModelForSequenceClassification.from_pretrained(self.model_name)
            self._model.eval()
            if self._torch_device is not None:
                self._model.to(self._torch_device)

        return self._tokenizer, self._model

    @staticmethod
    def _normalize_label(raw_label: Any) -> str:
        label = str(raw_label or "").strip().lower()
        if label in {"positive", "negative", "neutral"}:
            return label

        if label in {"label_0", "0", "0.0"} or "neg" in label:
            return "negative"
        if label in {"label_1", "1", "1.0"} or "neu" in label:
            return "neutral"
        if label in {"label_2", "2", "2.0"} or "pos" in label:
            return "positive"

        return "neutral"

    @staticmethod
    def _standard_output(label: str, score: float, model_name: str, model_version: str) -> dict:
        return {
            "label": label,
            "score": float(score),
            "model_name": model_name,
            "model_version": model_version,
        }

    def predict(self, text: str) -> dict:
        """Predict standardized sentiment output for one text."""

        normalized_text = clean_text(text, lowercase=False)
        if not normalized_text:
            return self._standard_output("neutral", 0.0, self.model_name, self.model_version)

        try:
            tokenizer, model = self._load_model()
            encoded_inputs = tokenizer(
                normalized_text,
                return_tensors="pt",
                truncation=True,
                max_length=self._max_length,
            )

            if self._torch_device is not None:
                encoded_inputs = {key: value.to(self._torch_device) for key, value in encoded_inputs.items()}

            with torch.inference_mode():
                outputs = model(**encoded_inputs)
                logits = outputs.logits
                probabilities = torch.softmax(logits, dim=-1)[0]

            best_index = int(torch.argmax(probabilities).item())
            id2label = getattr(model.config, "id2label", {}) or {}
            raw_label = id2label.get(best_index, id2label.get(str(best_index), f"LABEL_{best_index}"))
            label = self._normalize_label(raw_label)
            score = float(probabilities[best_index].item())
            return self._standard_output(label, score, self.model_name, self.model_version)
        except Exception as exc:
            print(f"[BertSentimentModel] Prediction error: {exc}")
            return self._standard_output("neutral", 0.0, self.model_name, self.model_version)

    def predict_batch(self, texts: list[str]) -> list[dict]:
        """Predict standardized sentiment output for a list of texts."""

        return [self.predict(text) for text in texts]

"""Fine-tuned BERT sentiment model loaded from a local folder."""

from __future__ import annotations

import json
from pathlib import Path
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


class TrainedBertSentimentModel(BaseSentimentModel):
    """Load a local fine-tuned model and return emotion labels as output."""

    def __init__(self, model_name: str | None = None, model_version: str = "v1", device: str = "cpu"):
        self.model_name = model_name or "models/fine_tuned_bert"
        self.model_version = model_version
        self.device = (device or "cpu").strip().lower()
        self._tokenizer = None
        self._model = None
        self._project_root = Path(__file__).resolve().parents[1]
        self._model_path = (self._project_root / self.model_name).resolve()
        self._torch_device = self._resolve_torch_device()
        self._max_length = 128
        self._model_display_name = self._model_path.name

        if torch is not None:
            try:
                torch.set_num_threads(1)
                torch.set_num_interop_threads(1)
            except Exception:
                pass

        self._load_metadata_if_present()

    def _load_metadata_if_present(self) -> None:
        metadata_path = self._model_path / "model_metadata.json"
        if not metadata_path.exists():
            return

        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            if isinstance(metadata, dict):
                model_name = metadata.get("model_name")
                if model_name and isinstance(model_name, str):
                    self._model_display_name = model_name

                training_config = metadata.get("training_config", {})
                if isinstance(training_config, dict):
                    max_length = training_config.get("max_length")
                    if isinstance(max_length, int) and max_length > 0:
                        self._max_length = max_length
        except Exception:
            # Metadata is optional for runtime inference.
            return

    def _resolve_torch_device(self):
        if torch is None:
            return None

        if self.device.startswith("cuda") and torch.cuda.is_available():
            return torch.device("cuda")

        if self.device.startswith("cuda"):
            print("[TrainedBertSentimentModel] CUDA requested but unavailable. Falling back to CPU.")

        return torch.device("cpu")

    def _load_model(self):
        if AutoTokenizer is None or AutoModelForSequenceClassification is None:
            raise RuntimeError(
                "transformers/torch are not installed. Run `pip install -r requirements.txt`."
            )

        if not self._model_path.exists():
            raise FileNotFoundError(
                f"Trained model folder not found: {self._model_path}. "
                "Put config.json/model.safetensors/tokenizer files in this folder."
            )

        if self._tokenizer is None or self._model is None:
            self._tokenizer = AutoTokenizer.from_pretrained(str(self._model_path), local_files_only=True)
            self._model = AutoModelForSequenceClassification.from_pretrained(
                str(self._model_path),
                local_files_only=True,
            )
            self._model.eval()
            if self._torch_device is not None:
                self._model.to(self._torch_device)

        return self._tokenizer, self._model

    @staticmethod
    def _normalize_emotion_label(label: Any) -> str:
        value = str(label or "").strip().lower()
        return value or "neutral"

    @staticmethod
    def _standard_output(label: str, score: float, model_name: str, model_version: str) -> dict:
        return {
            "label": label,
            "score": float(score),
            "model_name": model_name,
            "model_version": model_version,
        }

    def predict(self, text: str) -> dict:
        normalized_text = clean_text(text, lowercase=False)
        if not normalized_text:
            return self._standard_output("neutral", 0.0, self._model_display_name, self.model_version)

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
            id2label: dict[Any, Any] = getattr(model.config, "id2label", {}) or {}
            emotion_label = id2label.get(best_index, id2label.get(str(best_index), f"LABEL_{best_index}"))
            normalized_emotion = self._normalize_emotion_label(emotion_label)
            confidence = float(probabilities[best_index].item())

            return self._standard_output(
                normalized_emotion,
                confidence,
                self._model_display_name,
                self.model_version,
            )
        except Exception as exc:
            print(f"[TrainedBertSentimentModel] Prediction error: {exc}")
            return self._standard_output("neutral", 0.0, self._model_display_name, self.model_version)

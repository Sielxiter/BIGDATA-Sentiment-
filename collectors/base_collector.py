"""Base collector interface."""

from __future__ import annotations


class BaseCollector:
    """Interface for collectors that normalize source data for Kafka."""

    def collect(self):
        raise NotImplementedError

    def normalize(self, item: dict) -> dict:
        raise NotImplementedError

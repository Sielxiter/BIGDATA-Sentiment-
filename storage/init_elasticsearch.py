"""Initialize the Elasticsearch index for sentiment results."""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from storage.elastic_client import create_index_if_not_exists  # noqa: E402


def main() -> int:
    ready = create_index_if_not_exists()
    if ready:
        print("[init_elasticsearch] Elasticsearch index is ready")
        return 0

    print("[init_elasticsearch] Elasticsearch index could not be verified")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

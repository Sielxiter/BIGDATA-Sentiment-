"""Run the YouTube collector and send normalized comments to Kafka."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from collectors.live_youtube_collector import LiveYouTubeCollectorService  # noqa: E402
from config.settings import (  # noqa: E402
    DEFAULT_EVENT_NAME,
    DEFAULT_YOUTUBE_KEYWORDS,
    DEFAULT_YOUTUBE_MAX_COMMENTS,
    DEFAULT_YOUTUBE_MAX_VIDEOS,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect YouTube comments and send them to Kafka.")
    parser.add_argument(
        "--event",
        default=DEFAULT_EVENT_NAME,
        help="Event name used in the normalized Kafka messages.",
    )
    parser.add_argument(
        "--keywords",
        default=DEFAULT_YOUTUBE_KEYWORDS,
        help="Comma-separated YouTube search keywords.",
    )
    parser.add_argument(
        "--max-videos",
        type=int,
        default=DEFAULT_YOUTUBE_MAX_VIDEOS,
        help="Maximum number of videos to inspect per keyword.",
    )
    parser.add_argument(
        "--max-comments",
        type=int,
        default=DEFAULT_YOUTUBE_MAX_COMMENTS,
        help="Maximum number of comments to collect per video.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    keywords = [keyword.strip() for keyword in args.keywords.split(",") if keyword.strip()]

    try:
        service = LiveYouTubeCollectorService(
            event_name=args.event,
            keywords=keywords,
            max_videos=args.max_videos,
            max_comments=args.max_comments,
        )
        result = service.run_limited()
    except RuntimeError as exc:
        print(f"[run_youtube_collector] {exc}")
        return 1
    except Exception as exc:
        print(f"[run_youtube_collector] Collection failed: {exc}")
        return 1

    print(
        f"[run_youtube_collector] Sent {result.get('sent_count', 0)} message(s) "
        f"to Kafka topic raw_social_posts (session={result.get('session_id')})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

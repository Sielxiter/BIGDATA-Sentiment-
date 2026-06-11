"""Streamlit dashboard for YouTube-based sentiment collection."""

from __future__ import annotations

import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from elasticsearch import Elasticsearch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from cloud.youtube_api_collector import run_youtube_collection  # noqa: E402
from cloud.collector_config import load_collector_runtime_config, save_collector_runtime_config  # noqa: E402
from config.settings import settings  # noqa: E402
from dashboard.aws_lambda import LambdaInvokeError, invoke_youtube_collector_lambda  # noqa: E402


KAFKA_TOPIC = settings.kafka_raw_topic
LAMBDA_FUNCTION_NAME = settings.collector_lambda_function_name
LAMBDA_REGION = settings.collector_lambda_region
COLLECTOR_MODE = settings.collector_mode
COLLECTOR_SOURCE = settings.collector_source
SORT_OPTIONS = ["relevance", "date", "viewCount", "rating"]
SENTIMENT_CHART_COLORS = [
    "#43A5D8",
    "#E33163",
    "#FF7A1A",
    "#8CC84B",
    "#F7C948",
    "#7E57C2",
    "#26A69A",
    "#EF5350",
]
EVENT_FILTER_ALL = "All events"


def apply_simple_style() -> None:
    st.markdown(
        """
        <style>
        :root {
            --panel-bg: linear-gradient(180deg, #ffffff 0%, #f7fbff 100%);
            --panel-border: #d9e8f5;
            --ink: #102233;
            --muted: #637381;
            --accent: #1d8cf8;
        }
        .block-container {
            max-width: 1220px;
            padding-top: 1.2rem;
            padding-bottom: 1.8rem;
        }
        [data-testid="stMetricValue"] {
            font-size: 1.35rem;
        }
        .collector-badge {
            display: inline-block;
            border: 1px solid #bedcff;
            border-radius: 999px;
            padding: 0.35rem 0.8rem;
            background: #eef7ff;
            font-weight: 700;
            color: #1d4e89;
        }
        .hero-card {
            background: radial-gradient(circle at top left, #f5fbff 0%, #ffffff 58%, #f8fcff 100%);
            border: 1px solid var(--panel-border);
            border-radius: 22px;
            padding: 1.1rem 1.2rem;
            box-shadow: 0 12px 28px rgba(17, 24, 39, 0.06);
        }
        .hero-title {
            margin: 0;
            color: var(--ink);
            font-size: 1.8rem;
            font-weight: 800;
            letter-spacing: -0.03em;
        }
        .hero-subtitle {
            margin: 0.35rem 0 0 0;
            color: var(--muted);
            font-size: 0.98rem;
        }
        .config-pill {
            display: inline-block;
            margin-top: 0.8rem;
            margin-right: 0.55rem;
            padding: 0.42rem 0.7rem;
            border-radius: 14px;
            background: #ffffff;
            border: 1px solid #ddeaf7;
            color: #27445d;
            font-size: 0.92rem;
        }
        .section-card {
            background: var(--panel-bg);
            border: 1px solid var(--panel-border);
            border-radius: 22px;
            padding: 1rem 1rem 0.85rem 1rem;
            box-shadow: 0 10px 24px rgba(17, 24, 39, 0.05);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def format_int(value: Any) -> str:
    try:
        return f"{int(value):,}"
    except Exception:
        return "0"


@st.cache_resource(show_spinner=False)
def get_elasticsearch_client() -> Elasticsearch:
    return Elasticsearch(settings.elasticsearch_url)


@st.cache_data(ttl=5, show_spinner=False)
def load_available_events(limit: int = 20) -> tuple[list[str], str | None]:
    try:
        client = get_elasticsearch_client()
        if not client.indices.exists(index=settings.elasticsearch_index):
            return [EVENT_FILTER_ALL], "Elasticsearch index does not exist yet."

        response = client.search(
            index=settings.elasticsearch_index,
            size=0,
            aggs={
                "event_names": {
                    "terms": {
                        "field": "event_name",
                        "size": limit,
                        "order": {"_count": "desc"},
                    }
                }
            },
        )
        buckets = response.get("aggregations", {}).get("event_names", {}).get("buckets", [])
        events = [str(bucket.get("key", "")).strip() for bucket in buckets if str(bucket.get("key", "")).strip()]
        if not events:
            events = [EVENT_FILTER_ALL]
        else:
            events = [EVENT_FILTER_ALL] + events
        return events, None
    except Exception as exc:
        return [EVENT_FILTER_ALL], str(exc)


@st.cache_data(ttl=5, show_spinner=False)
def load_sentiment_counts(limit: int = 12, event_name: str | None = None) -> tuple[pd.DataFrame, int, str | None]:
    try:
        client = get_elasticsearch_client()
        if not client.indices.exists(index=settings.elasticsearch_index):
            return pd.DataFrame(columns=["label", "count"]), 0, "Elasticsearch index does not exist yet."

        query: dict[str, Any] = {"match_all": {}}
        if event_name and event_name != EVENT_FILTER_ALL:
            query = {"term": {"event_name": event_name}}

        response = client.search(
            index=settings.elasticsearch_index,
            size=0,
            query=query,
            aggs={
                "sentiment_counts": {
                    "terms": {
                        "field": "sentiment",
                        "size": limit,
                        "order": {"_count": "desc"},
                    }
                }
            },
        )
        buckets = response.get("aggregations", {}).get("sentiment_counts", {}).get("buckets", [])
        rows = [{"label": bucket.get("key", "unknown"), "count": bucket.get("doc_count", 0)} for bucket in buckets]
        total = response.get("hits", {}).get("total", {})
        total_count = int(total.get("value", 0)) if isinstance(total, dict) else int(total or 0)
        return pd.DataFrame(rows, columns=["label", "count"]), total_count, None
    except Exception as exc:
        return pd.DataFrame(columns=["label", "count"]), 0, str(exc)


@st.cache_data(ttl=10, show_spinner=False)
def load_recent_comments(limit: int = 20, event_name: str | None = None) -> tuple[pd.DataFrame, str | None]:
    try:
        client = get_elasticsearch_client()
        if not client.indices.exists(index=settings.elasticsearch_index):
            return pd.DataFrame(), "Elasticsearch index does not exist yet."

        query: dict[str, Any] = {"match_all": {}}
        if event_name and event_name != EVENT_FILTER_ALL:
            query = {"term": {"event_name": event_name}}

        response = client.search(
            index=settings.elasticsearch_index,
            size=limit,
            query=query,
            sort=[{"processed_at": {"order": "desc"}}],
            _source=[
                "event_name",
                "original_text",
                "sentiment",
                "confidence",
                "author",
                "video_title",
                "processed_at",
            ],
        )
        rows: list[dict[str, Any]] = []
        for hit in response.get("hits", {}).get("hits", []):
            source = hit.get("_source", {}) or {}
            rows.append(
                {
                    "Event": source.get("event_name", ""),
                    "Comment": source.get("original_text", ""),
                    "Sentiment": source.get("sentiment", ""),
                    "Confidence": round(float(source.get("confidence", 0.0) or 0.0), 3),
                    "Author": source.get("author", ""),
                    "Video title": source.get("video_title", ""),
                    "Processed at": source.get("processed_at", ""),
                }
            )
        return pd.DataFrame(rows), None
    except Exception as exc:
        return pd.DataFrame(), str(exc)


@st.cache_data(ttl=20, show_spinner=False)
def load_saved_collector_config() -> tuple[dict[str, str], str | None]:
    config, error = load_collector_runtime_config(
        bucket=settings.collector_config_bucket_name,
        key=settings.collector_config_s3_key,
        region=settings.aws_region,
    )
    normalized = {
        "event_name": str(config.get("event_name") or settings.default_event_name).strip(),
        "search_query": str(
            config.get("search_query")
            or config.get("keywords")
            or settings.default_youtube_search_queries
        ).strip(),
    }
    return normalized, error


def build_sentiment_bar_chart(counts_df: pd.DataFrame) -> go.Figure:
    if counts_df.empty:
        labels = ["No data"]
        values = [0]
    else:
        labels = counts_df["label"].astype(str).tolist()
        values = counts_df["count"].astype(int).tolist()

    colors = [SENTIMENT_CHART_COLORS[index % len(SENTIMENT_CHART_COLORS)] for index in range(len(labels))]

    fig = go.Figure(
        data=[
            go.Bar(
                x=labels,
                y=values,
                marker={"color": colors, "line": {"color": "#111111", "width": 3}},
                text=values,
                textposition="outside",
                hovertemplate="<b>%{x}</b><br>Comments: %{y}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title={"text": "Real-Time Sentiment Pulse", "x": 0.02, "xanchor": "left", "font": {"size": 22}},
        height=460,
        margin={"l": 28, "r": 12, "t": 78, "b": 55},
        plot_bgcolor="#F7FBFF",
        paper_bgcolor="#F7FBFF",
        font={"family": "Trebuchet MS, Segoe UI, sans-serif", "size": 14, "color": "#19324D"},
        bargap=0.28,
        showlegend=False,
    )
    fig.update_xaxes(
        title="Sentiment / Emotion",
        tickangle=0,
        showline=True,
        linewidth=2,
        linecolor="#8CB8DE",
        ticks="outside",
        tickwidth=2,
        tickcolor="#8CB8DE",
    )
    fig.update_yaxes(
        title="Number of comments",
        rangemode="tozero",
        showline=True,
        linewidth=2,
        linecolor="#8CB8DE",
        ticks="outside",
        tickwidth=2,
        tickcolor="#8CB8DE",
        gridcolor="#DCEAF7",
        zeroline=False,
    )
    return fig


def _extract_lambda_body(result: dict[str, Any]) -> dict[str, Any]:
    decoded = result.get("decoded", {}) or {}
    body = decoded.get("body_parsed") if isinstance(decoded, dict) else None
    if not isinstance(body, dict):
        body = decoded if isinstance(decoded, dict) else {}
    return body


def render_success(result: dict[str, Any]) -> None:
    body = _extract_lambda_body(result)
    if not body:
        body = result

    posts_scanned = body.get("videos_scanned", body.get("posts_scanned"))
    comments_collected = body.get("comments_collected")
    messages_published = body.get("messages_published", comments_collected)
    session_id = body.get("session_id")
    kafka_topic = body.get("kafka_topic", KAFKA_TOPIC)
    s3_backup_key = body.get("s3_backup_key")
    backup_error = body.get("backup_error")
    errors = body.get("errors") or body.get("warnings") or []

    if body.get("status") == "ok":
        st.success("YouTube comments collected successfully.")
    else:
        st.warning("Collection finished with warnings.")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Videos scanned", format_int(posts_scanned))
    c2.metric("Comments collected", format_int(comments_collected))
    c3.metric("Published", format_int(messages_published))
    c4.metric("Topic", str(kafka_topic or KAFKA_TOPIC))

    if session_id:
        st.markdown("### Session")
        st.code(str(session_id), language="text")

    if s3_backup_key:
        st.markdown("### S3 Backup")
        st.code(str(s3_backup_key), language="text")
        st.caption("This JSONL object stores the raw YouTube comments.")
    elif backup_error:
        st.warning(f"S3 backup issue: {backup_error}")

    if errors:
        st.markdown("### Warnings")
        for error in errors[:5]:
            st.write(f"- {error}")

    st.session_state["last_response"] = result


def render_error(exc: Exception) -> None:
    st.error(str(exc))
    st.caption("If this is a permissions issue: ensure your local IAM user has `lambda:InvokeFunction`.")
    details = getattr(exc, "details", None)
    if isinstance(details, dict) and details:
        with st.expander("Details"):
            st.json(details)


def render_last_response() -> None:
    last_result = st.session_state.get("last_response")
    if not last_result:
        return

    with st.expander("Last response"):
        st.json(last_result)


def render_live_sentiment_panel() -> None:
    selected_event = st.session_state.get("selected_event_filter", EVENT_FILTER_ALL)
    counts_df, total_count, error = load_sentiment_counts(event_name=selected_event)

    st.markdown("### Real-Time Results")
    st.caption("Live sentiment counts from Elasticsearch.")
    st.write(f"Event filter: `{selected_event}`")

    metric_left, metric_right = st.columns(2)
    metric_left.metric("Processed comments", format_int(total_count))
    metric_right.metric("Labels visible", format_int(len(counts_df)))

    if error:
        st.warning(f"Live chart is waiting for Elasticsearch data: {error}")

    fig = build_sentiment_bar_chart(counts_df)
    st.plotly_chart(fig, width="stretch", key="live_sentiment_distribution_bar")

    if counts_df.empty:
        st.info("No processed comments yet. Collect data, then run Kafka, Spark, BERT, and Elasticsearch.")
    else:
        top_label = counts_df.iloc[0]["label"]
        top_count = int(counts_df.iloc[0]["count"])
        st.success(f"Current most common label: `{top_label}` with {top_count} comments.")

    st.caption(f"Last refresh: {datetime.now().strftime('%H:%M:%S')}")


def render_recent_comments_panel() -> None:
    selected_event = st.session_state.get("selected_event_filter", EVENT_FILTER_ALL)
    comments_df, error = load_recent_comments(event_name=selected_event)

    st.markdown("### Latest Processed Comments")
    st.caption("Most recent comments already processed by Spark and indexed in Elasticsearch.")

    if error:
        st.warning(f"Comments table is waiting for Elasticsearch data: {error}")
        return

    if comments_df.empty:
        st.info("No processed comments available yet.")
        return

    st.dataframe(
        comments_df,
        width="stretch",
        hide_index=True,
        column_config={
            "Comment": st.column_config.TextColumn(width="large"),
            "Video title": st.column_config.TextColumn(width="medium"),
            "Confidence": st.column_config.NumberColumn(format="%.3f"),
        },
    )


def invoke_local_collection(
    *,
    event_name: str,
    search_query: str,
) -> dict[str, Any]:
    return run_youtube_collection(
        event_name=event_name,
        search_query=search_query,
        video_ids=settings.default_youtube_video_ids,
        channel_ids=settings.default_youtube_channel_ids,
        max_videos=settings.youtube_max_videos,
        max_comments_per_video=settings.youtube_max_comments_per_video,
        order=settings.default_youtube_order,
        region_code=settings.default_youtube_region_code or None,
        relevance_language=settings.default_youtube_relevance_language or None,
        published_after=settings.youtube_published_after or None,
        published_before=settings.youtube_published_before or None,
        engine="youtube_api",
        publish_to_kafka=True,
        backup_to_s3=settings.enable_s3_backup,
        include_documents=False,
        kafka_topic=settings.kafka_raw_topic,
        kafka_bootstrap_servers=settings.kafka_bootstrap_servers,
        s3_bucket=settings.s3_bucket_name,
        s3_prefix=settings.s3_prefix,
        aws_region=settings.aws_region,
    )


def invoke_lambda_collection(
    *,
    event_name: str,
    search_query: str,
) -> dict[str, Any]:
    result = invoke_youtube_collector_lambda(
        event_name=event_name,
        search_query=search_query,
        video_ids="",
        channel_ids="",
        max_videos=settings.youtube_max_videos,
        max_comments_per_video=settings.youtube_max_comments_per_video,
        order=settings.default_youtube_order,
        region_code=settings.default_youtube_region_code,
        relevance_language=settings.default_youtube_relevance_language,
        published_after=settings.youtube_published_after,
        published_before=settings.youtube_published_before,
        engine="youtube_api",
        function_name=LAMBDA_FUNCTION_NAME,
        region_name=LAMBDA_REGION,
    )
    return result


def build_collector_runtime_config(
    *,
    event_name: str,
    search_query: str,
) -> dict[str, Any]:
    return {
        "event_name": event_name,
        "search_query": search_query,
    }


@st.dialog("Edit collection settings")
def render_config_dialog(initial_config: dict[str, str]) -> None:
    with st.form("edit_collection_settings_form", clear_on_submit=False):
        event_name = st.text_input("Event name", value=initial_config.get("event_name", settings.default_event_name))
        search_query = st.text_input(
            "Search queries (comma-separated)",
            value=initial_config.get("search_query", settings.default_youtube_search_queries),
        )
        submitted = st.form_submit_button("Save settings")

    if submitted:
        if not event_name.strip():
            st.error("Event name is required.")
            return
        if not search_query.strip():
            st.error("Search query is required.")
            return

        runtime_config = build_collector_runtime_config(
            event_name=event_name.strip(),
            search_query=search_query.strip(),
        )
        saved_key, save_error = save_collector_runtime_config(
            runtime_config,
            bucket=settings.collector_config_bucket_name,
            key=settings.collector_config_s3_key,
            region=settings.aws_region,
        )
        if save_error:
            st.error(f"Could not save collector settings: {save_error}")
            return

        st.session_state["collector_runtime_config"] = runtime_config
        load_saved_collector_config.clear()
        st.session_state["config_save_message"] = f"Saved collector settings to `{saved_key}`."
        st.rerun()


def main() -> None:
    st.set_page_config(page_title="YouTube Sentiment Dashboard", layout="wide", initial_sidebar_state="collapsed")
    apply_simple_style()

    with st.sidebar:
        st.markdown("### Live Refresh")
        auto_refresh = st.toggle("Auto refresh", value=True)
        refresh_seconds = st.number_input("Refresh every seconds", min_value=3, max_value=60, value=5, step=1)
        st.caption("Disable while typing if the page refreshes too often.")

    saved_runtime_config, config_error = load_saved_collector_config()
    if "collector_runtime_config" not in st.session_state:
        st.session_state["collector_runtime_config"] = saved_runtime_config
    runtime_config = st.session_state["collector_runtime_config"]

    st.markdown(
        f"""
        <div class="hero-card">
            <div class="collector-badge">Collector mode: {COLLECTOR_MODE}</div>
            <h1 class="hero-title">Sentiment Monitoring Dashboard</h1>
            <p class="hero-subtitle">Track processed comments in real time and manage the global collection target from one place.</p>
            <div class="config-pill"><strong>Event:</strong> {runtime_config.get("event_name", settings.default_event_name)}</div>
            <div class="config-pill"><strong>Queries:</strong> {runtime_config.get("search_query", settings.default_youtube_search_queries)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if config_error:
        st.warning(f"Collector config warning: {config_error}")
    if st.session_state.get("config_save_message"):
        st.success(st.session_state.pop("config_save_message"))

    available_events, event_error = load_available_events()
    if "selected_event_filter" not in st.session_state:
        st.session_state["selected_event_filter"] = EVENT_FILTER_ALL

    filter_left, filter_right = st.columns([0.62, 0.38])
    with filter_left:
        current_event = st.selectbox(
            "Filter live chart by event",
            available_events,
            index=available_events.index(st.session_state["selected_event_filter"])
            if st.session_state["selected_event_filter"] in available_events
            else 0,
            key="selected_event_filter",
        )
    with filter_right:
        st.markdown("### Live Filter")
        st.write(f"Current event: `{current_event}`")
        if event_error:
            st.caption(f"Filter source warning: {event_error}")

    action_left, action_right = st.columns([0.68, 0.32])
    with action_left:
        st.markdown("### Collection Controls")
        st.caption("The saved event and search queries are global and reused by scheduled collectors.")
    with action_right:
        button_left, button_right = st.columns(2)
        with button_left:
            if st.button("Edit settings", width="stretch"):
                render_config_dialog(runtime_config)
        with button_right:
            collect_label = "Run Lambda now" if COLLECTOR_MODE == "lambda" else "Collect now"
            collect_now = st.button(collect_label, width="stretch")

    if collect_now:
        with st.spinner("Running collection..."):
            try:
                saved_key, save_error = save_collector_runtime_config(
                    runtime_config,
                    bucket=settings.collector_config_bucket_name,
                    key=settings.collector_config_s3_key,
                    region=settings.aws_region,
                )
                if save_error:
                    st.error(f"Could not save collector settings: {save_error}")
                else:
                    if COLLECTOR_MODE == "lambda":
                        result = invoke_lambda_collection(
                            event_name=runtime_config["event_name"],
                            search_query=runtime_config["search_query"],
                        )
                    else:
                        result = invoke_local_collection(
                            event_name=runtime_config["event_name"],
                            search_query=runtime_config["search_query"],
                        )
                    st.success(f"Using collector settings from `{saved_key}`.")
                    render_success(result)
            except LambdaInvokeError as exc:
                render_error(exc)
            except Exception as exc:
                st.error(f"Unexpected error while running collection: {exc}")

    render_last_response()

    top_left, top_right = st.columns([1.08, 0.92])
    with top_left:
        with st.container(border=False):
            st.markdown("<div class='section-card'>", unsafe_allow_html=True)
            render_live_sentiment_panel()
            st.markdown("</div>", unsafe_allow_html=True)
    with top_right:
        with st.container(border=False):
            st.markdown("<div class='section-card'>", unsafe_allow_html=True)
            render_recent_comments_panel()
            st.markdown("</div>", unsafe_allow_html=True)

    if auto_refresh:
        time.sleep(int(refresh_seconds))
        st.rerun()


if __name__ == "__main__":
    main()

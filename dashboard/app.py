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
from cloud.collector_config import save_collector_runtime_config  # noqa: E402
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
        .block-container {
            max-width: 1180px;
            padding-top: 1rem;
            padding-bottom: 1.5rem;
        }
        [data-testid="stMetricValue"] {
            font-size: 1.2rem;
        }
        .collector-badge {
            display: inline-block;
            border: 2px solid #111111;
            border-radius: 999px;
            padding: 0.2rem 0.65rem;
            background: #f7f7f7;
            font-weight: 700;
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
        title={"text": "Live Sentiment Distribution", "x": 0.02, "xanchor": "left"},
        height=430,
        margin={"l": 50, "r": 20, "t": 70, "b": 80},
        plot_bgcolor="#FFFFFF",
        paper_bgcolor="#FFFFFF",
        font={"family": "Arial Black, Arial, sans-serif", "size": 14, "color": "#222222"},
        bargap=0.35,
        showlegend=False,
    )
    fig.update_xaxes(
        title="Sentiment / Emotion",
        tickangle=0,
        showline=True,
        linewidth=4,
        linecolor="#111111",
        ticks="outside",
        tickwidth=3,
        tickcolor="#111111",
    )
    fig.update_yaxes(
        title="Number of comments",
        rangemode="tozero",
        showline=True,
        linewidth=4,
        linecolor="#111111",
        ticks="outside",
        tickwidth=3,
        tickcolor="#111111",
        gridcolor="#E6E6E6",
        zeroline=False,
    )
    return fig


def render_target_summary() -> None:
    st.markdown("### Target")
    st.write(f"Source: `{COLLECTOR_SOURCE}`")
    st.write(f"Mode: `{COLLECTOR_MODE}`")
    if COLLECTOR_MODE == "lambda":
        st.write(f"Lambda: `{LAMBDA_FUNCTION_NAME}`")
        st.write(f"Region: `{LAMBDA_REGION}`")
        if settings.collector_config_bucket_name:
            st.write(f"Saved collector config: `s3://{settings.collector_config_bucket_name}/{settings.collector_config_s3_key}`")
    st.write(f"Kafka topic: `{KAFKA_TOPIC}`")
    if settings.s3_bucket_name:
        st.write(f"S3 bucket: `{settings.s3_bucket_name}`")
        st.write(f"S3 prefix: `{settings.s3_prefix}`")
    st.caption("AWS credentials come from the EC2 instance profile or configured AWS CLI credentials.")


def render_flow_summary() -> None:
    st.markdown("### Flow")
    if COLLECTOR_MODE == "lambda":
        flow = "Streamlit -> S3 saved config -> EventBridge / Streamlit invoke -> AWS Lambda -> YouTube Data API -> Kafka -> Spark -> Elasticsearch"
    else:
        flow = "Streamlit -> local YouTube API collector -> Kafka -> Spark -> Elasticsearch"
    st.code(flow, language="text")
    st.info(
        "The collector uses the YouTube Data API and requires a valid YOUTUBE_API_KEY."
    )


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
    st.caption(
        "This graph refreshes from Elasticsearch, where Spark writes BERT predictions after consuming Kafka."
    )
    st.write(f"Event filter: `{selected_event}`")

    metric_left, metric_right = st.columns(2)
    metric_left.metric("Processed comments", format_int(total_count))
    metric_right.metric("Labels visible", format_int(len(counts_df)))

    if error:
        st.warning(f"Live chart is waiting for Elasticsearch data: {error}")

    fig = build_sentiment_bar_chart(counts_df)
    st.plotly_chart(fig, use_container_width=True, key="live_sentiment_distribution_bar")

    if counts_df.empty:
        st.info("No processed comments yet. Collect data, then run Kafka, Spark, BERT, and Elasticsearch.")
    else:
        top_label = counts_df.iloc[0]["label"]
        top_count = int(counts_df.iloc[0]["count"])
        st.success(f"Current most common label: `{top_label}` with {top_count} comments.")

    st.caption(f"Last refresh: {datetime.now().strftime('%H:%M:%S')}")


def invoke_local_collection(
    *,
    event_name: str,
    search_query: str,
    video_ids: str,
    channel_ids: str,
    max_videos: int,
    max_comments_per_video: int,
    order: str,
    region_code: str,
    relevance_language: str,
    published_after: str,
    published_before: str,
    engine: str,
) -> dict[str, Any]:
    return run_youtube_collection(
        event_name=event_name,
        search_query=search_query,
        video_ids=video_ids,
        channel_ids=channel_ids,
        max_videos=max_videos,
        max_comments_per_video=max_comments_per_video,
        order=order,
        region_code=region_code or None,
        relevance_language=relevance_language or None,
        published_after=published_after or None,
        published_before=published_before or None,
        engine=engine,
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
    video_ids: str,
    channel_ids: str,
    max_videos: int,
    max_comments_per_video: int,
    order: str,
    region_code: str,
    relevance_language: str,
    published_after: str,
    published_before: str,
    engine: str,
) -> dict[str, Any]:
    result = invoke_youtube_collector_lambda(
        event_name=event_name,
        search_query=search_query,
        video_ids=video_ids,
        channel_ids=channel_ids,
        max_videos=max_videos,
        max_comments_per_video=max_comments_per_video,
        order=order,
        region_code=region_code,
        relevance_language=relevance_language,
        published_after=published_after,
        published_before=published_before,
        engine=engine,
        function_name=LAMBDA_FUNCTION_NAME,
        region_name=LAMBDA_REGION,
    )
    return result


def build_collector_runtime_config(
    *,
    event_name: str,
    search_query: str,
    video_ids: str,
    channel_ids: str,
    max_videos: int,
    max_comments_per_video: int,
    order: str,
    region_code: str,
    relevance_language: str,
    published_after: str,
    published_before: str,
    engine: str,
) -> dict[str, Any]:
    return {
        "event_name": event_name,
        "search_query": search_query,
        "video_ids": video_ids,
        "channel_ids": channel_ids,
        "max_videos": int(max_videos),
        "max_comments_per_video": int(max_comments_per_video),
        "order": order,
        "region_code": region_code,
        "relevance_language": relevance_language,
        "published_after": published_after,
        "published_before": published_before,
        "engine": engine,
    }


def main() -> None:
    st.set_page_config(page_title="YouTube Sentiment Dashboard", layout="wide", initial_sidebar_state="collapsed")
    apply_simple_style()

    with st.sidebar:
        st.markdown("### Live Refresh")
        auto_refresh = st.toggle("Auto refresh", value=True)
        refresh_seconds = st.number_input("Refresh every seconds", min_value=3, max_value=60, value=5, step=1)
        st.caption("Disable while typing if the page refreshes too often.")
        st.markdown("### Deployment")
        st.write("Use `COLLECTOR_MODE=lambda` for AWS-triggered collection.")
        st.write("Use the top event filter to narrow live Elasticsearch charts.")

    st.title("YouTube Sentiment Dashboard")
    st.caption("Collect public YouTube comments, publish them to Kafka, and watch Spark/BERT results update live.")
    st.markdown(f"<span class='collector-badge'>Collector mode: {COLLECTOR_MODE}</span>", unsafe_allow_html=True)

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

    left, right = st.columns([0.95, 1.05])

    with left:
        with st.container(border=True):
            st.markdown("### Collect YouTube Data")
            if COLLECTOR_MODE == "lambda":
                st.info(
                    "Lambda mode saves the chosen event/query settings to S3 so your scheduled EventBridge runs use them. "
                    "This button also runs Lambda immediately after saving."
                )
            else:
                st.info("Local mode runs the collector directly from Streamlit.")

            with st.form("youtube_collector_form", clear_on_submit=False):
                event_name = st.text_input("Event name", value=settings.default_event_name)
                search_query_raw = st.text_input(
                    "Search queries (comma-separated)",
                    value=settings.default_youtube_search_queries,
                )
                video_ids_raw = st.text_input(
                    "Video IDs (comma-separated, optional)",
                    value=settings.default_youtube_video_ids,
                )
                channel_ids_raw = st.text_input(
                    "Channel IDs (comma-separated, optional)",
                    value=settings.default_youtube_channel_ids,
                )
                c1, c2 = st.columns(2)
                with c1:
                    max_videos = st.number_input("Max videos", min_value=1, max_value=200, value=settings.youtube_max_videos, step=1)
                with c2:
                    max_comments = st.number_input(
                        "Max comments per post",
                        min_value=1,
                        max_value=200,
                        value=settings.youtube_max_comments_per_video,
                        step=1,
                    )

                c3, c4 = st.columns(2)
                with c3:
                    order = st.selectbox(
                        "Order",
                        SORT_OPTIONS,
                        index=SORT_OPTIONS.index(settings.default_youtube_order)
                        if settings.default_youtube_order in SORT_OPTIONS
                        else 0,
                    )
                with c4:
                    region_code = st.text_input(
                        "Region code (optional)",
                        value=settings.default_youtube_region_code,
                    )

                c5, c6 = st.columns(2)
                with c5:
                    relevance_language = st.text_input(
                        "Relevance language (optional)",
                        value=settings.default_youtube_relevance_language,
                    )
                with c6:
                    engine = st.text_input(
                        "Engine label",
                        value="youtube_api",
                        help="Stored as metadata for traceability.",
                    )

                published_after = st.text_input(
                    "Published after (optional, ISO 8601)",
                    value=settings.youtube_published_after,
                )
                published_before = st.text_input(
                    "Published before (optional, ISO 8601)",
                    value=settings.youtube_published_before,
                )

                submit_label = "Save settings & run Lambda now" if COLLECTOR_MODE == "lambda" else "Collect YouTube Comments"
                submitted = st.form_submit_button(submit_label, use_container_width=True)

            if submitted:
                if not event_name.strip():
                    st.error("Event name is required.")
                elif not search_query_raw.strip() and not video_ids_raw.strip():
                    st.error("Provide at least one search query or video ID.")
                else:
                    runtime_config = build_collector_runtime_config(
                        event_name=event_name.strip(),
                        search_query=search_query_raw.strip(),
                        video_ids=video_ids_raw.strip(),
                        channel_ids=channel_ids_raw.strip(),
                        max_videos=int(max_videos),
                        max_comments_per_video=int(max_comments),
                        order=order,
                        region_code=region_code.strip(),
                        relevance_language=relevance_language.strip(),
                        published_after=published_after.strip(),
                        published_before=published_before.strip(),
                        engine=engine,
                    )
                    with st.spinner(
                        "Collecting YouTube comments..."
                        if COLLECTOR_MODE != "lambda"
                        else "Saving settings and triggering Lambda..."
                    ):
                        try:
                            if COLLECTOR_MODE == "lambda":
                                saved_key, save_error = save_collector_runtime_config(
                                    runtime_config,
                                    bucket=settings.collector_config_bucket_name,
                                    key=settings.collector_config_s3_key,
                                    region=settings.aws_region,
                                )
                                if save_error:
                                    st.error(f"Could not save Lambda collector settings: {save_error}")
                                else:
                                    result = invoke_lambda_collection(
                                        event_name=runtime_config["event_name"],
                                        search_query=runtime_config["search_query"],
                                        video_ids=runtime_config["video_ids"],
                                        channel_ids=runtime_config["channel_ids"],
                                        max_videos=runtime_config["max_videos"],
                                        max_comments_per_video=runtime_config["max_comments_per_video"],
                                        order=runtime_config["order"],
                                        region_code=runtime_config["region_code"],
                                        relevance_language=runtime_config["relevance_language"],
                                        published_after=runtime_config["published_after"],
                                        published_before=runtime_config["published_before"],
                                        engine=runtime_config["engine"],
                                    )
                                    body = _extract_lambda_body(result)
                                    if body.get("status") == "ok":
                                        st.success(f"Saved collector settings to `{saved_key}`.")
                                        render_success(result)
                                    else:
                                        message = body.get("message") or "Lambda returned an error."
                                        st.error(str(message))
                                        error_items = body.get("errors") or body.get("warnings") or []
                                        if error_items:
                                            with st.expander("Details"):
                                                if isinstance(error_items, list):
                                                    for error in error_items:
                                                        st.write(f"- {error}")
                                                else:
                                                    st.json(error_items)
                            else:
                                result = invoke_local_collection(
                                    event_name=runtime_config["event_name"],
                                    search_query=runtime_config["search_query"],
                                    video_ids=runtime_config["video_ids"],
                                    channel_ids=runtime_config["channel_ids"],
                                    max_videos=runtime_config["max_videos"],
                                    max_comments_per_video=runtime_config["max_comments_per_video"],
                                    order=runtime_config["order"],
                                    region_code=runtime_config["region_code"],
                                    relevance_language=runtime_config["relevance_language"],
                                    published_after=runtime_config["published_after"],
                                    published_before=runtime_config["published_before"],
                                    engine=runtime_config["engine"],
                                )
                                render_success(result)
                        except LambdaInvokeError as exc:
                            render_error(exc)
                        except Exception as exc:
                            st.error(f"Unexpected error while collecting YouTube data: {exc}")

            render_last_response()

    with right:
        with st.container(border=True):
            render_live_sentiment_panel()

    with st.expander("Workflow and AWS target"):
        info_left, info_right = st.columns(2)
        with info_left:
            render_target_summary()
        with info_right:
            render_flow_summary()
            st.markdown("### Notes")
            st.write("- Streamlit on EC2 should use an instance profile or configured AWS CLI credentials.")
            st.write("- The collector uses the YouTube Data API and requires an API key with quota available.")
            st.write("- Spark reads Kafka, applies the BERT model, and writes processed results to Elasticsearch.")

    if auto_refresh:
        time.sleep(int(refresh_seconds))
        st.rerun()


if __name__ == "__main__":
    main()

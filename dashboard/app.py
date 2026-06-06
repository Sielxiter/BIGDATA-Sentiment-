"""Streamlit dashboard for Reddit-based sentiment collection."""

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

from cloud.reddit_scraper_collector import run_reddit_collection  # noqa: E402
from config.settings import settings  # noqa: E402
from dashboard.aws_lambda import LambdaInvokeError, invoke_reddit_collector_lambda  # noqa: E402


KAFKA_TOPIC = settings.kafka_raw_topic
LAMBDA_FUNCTION_NAME = settings.collector_lambda_function_name
LAMBDA_REGION = settings.collector_lambda_region
COLLECTOR_MODE = settings.collector_mode
COLLECTOR_SOURCE = settings.collector_source
SORT_OPTIONS = ["relevance", "hot", "new", "top"]
TIME_FILTER_OPTIONS = ["", "hour", "day", "week", "month", "year", "all"]
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


def load_sentiment_counts(limit: int = 12) -> tuple[pd.DataFrame, int, str | None]:
    try:
        client = get_elasticsearch_client()
        if not client.indices.exists(index=settings.elasticsearch_index):
            return pd.DataFrame(columns=["label", "count"]), 0, "Elasticsearch index does not exist yet."

        response = client.search(
            index=settings.elasticsearch_index,
            size=0,
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
    st.write(f"Kafka topic: `{KAFKA_TOPIC}`")
    if settings.s3_bucket_name:
        st.write(f"S3 bucket: `{settings.s3_bucket_name}`")
        st.write(f"S3 prefix: `{settings.s3_prefix}`")
    st.caption("AWS credentials come from the EC2 instance profile or configured AWS CLI credentials.")


def render_flow_summary() -> None:
    st.markdown("### Flow")
    if COLLECTOR_MODE == "lambda":
        flow = "Streamlit -> AWS Lambda -> Reddit public pages -> Kafka -> Spark -> Elasticsearch"
    else:
        flow = "Streamlit -> local Reddit scraper -> Kafka -> Spark -> Elasticsearch"
    st.code(flow, language="text")
    st.info(
        "The collector uses public Reddit pages and JSON endpoints. It does not require Reddit API credentials."
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

    posts_scanned = body.get("posts_scanned")
    comments_collected = body.get("comments_collected")
    messages_published = body.get("messages_published", comments_collected)
    session_id = body.get("session_id")
    kafka_topic = body.get("kafka_topic", KAFKA_TOPIC)
    s3_backup_key = body.get("s3_backup_key")
    backup_error = body.get("backup_error")
    errors = body.get("errors") or body.get("warnings") or []

    if body.get("status") == "ok":
        st.success("Reddit comments collected successfully.")
    else:
        st.warning("Collection finished with warnings.")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Posts scanned", format_int(posts_scanned))
    c2.metric("Comments collected", format_int(comments_collected))
    c3.metric("Published", format_int(messages_published))
    c4.metric("Topic", str(kafka_topic or KAFKA_TOPIC))

    if session_id:
        st.markdown("### Session")
        st.code(str(session_id), language="text")

    if s3_backup_key:
        st.markdown("### S3 Backup")
        st.code(str(s3_backup_key), language="text")
        st.caption("This JSONL object stores the raw Reddit comments.")
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
    counts_df, total_count, error = load_sentiment_counts()

    st.markdown("### Real-Time Results")
    st.caption(
        "This graph refreshes from Elasticsearch, where Spark writes BERT predictions after consuming Kafka."
    )

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
    keywords: str,
    subreddits: str,
    max_posts: int,
    max_comments_per_post: int,
    sort_mode: str,
    time_filter: str,
    engine: str,
) -> dict[str, Any]:
    return run_reddit_collection(
        event_name=event_name,
        keywords=keywords,
        subreddits=subreddits,
        max_posts=max_posts,
        max_comments_per_post=max_comments_per_post,
        sort_mode=sort_mode,
        time_filter=time_filter or None,
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
    keywords: str,
    subreddits: str,
    max_posts: int,
    max_comments_per_post: int,
    sort_mode: str,
    time_filter: str,
    engine: str,
) -> dict[str, Any]:
    result = invoke_reddit_collector_lambda(
        event_name=event_name,
        keywords=keywords,
        subreddits=subreddits,
        max_posts=max_posts,
        max_comments_per_post=max_comments_per_post,
        sort_mode=sort_mode,
        time_filter=time_filter,
        engine=engine,
        function_name=LAMBDA_FUNCTION_NAME,
        region_name=LAMBDA_REGION,
    )
    return result


def main() -> None:
    st.set_page_config(page_title="Reddit Sentiment Dashboard", layout="wide", initial_sidebar_state="collapsed")
    apply_simple_style()

    with st.sidebar:
        st.markdown("### Live Refresh")
        auto_refresh = st.toggle("Auto refresh", value=True)
        refresh_seconds = st.number_input("Refresh every seconds", min_value=3, max_value=60, value=5, step=1)
        st.caption("Disable while typing if the page refreshes too often.")

    st.title("Reddit Sentiment Dashboard")
    st.caption("Collect public Reddit comments, publish them to Kafka, and watch Spark/BERT results update live.")
    st.markdown(f"<span class='collector-badge'>Collector mode: {COLLECTOR_MODE}</span>", unsafe_allow_html=True)

    left, right = st.columns([0.95, 1.05])

    with left:
        with st.container(border=True):
            st.markdown("### Collect Reddit Data")

            with st.form("reddit_collector_form", clear_on_submit=False):
                event_name = st.text_input("Event name", value=settings.default_event_name)
                keywords_raw = st.text_input("Keywords (comma-separated)", value=settings.default_reddit_keywords)
                subreddits_raw = st.text_input(
                    "Subreddits (comma-separated, optional)",
                    value=settings.default_reddit_subreddits,
                )
                c1, c2 = st.columns(2)
                with c1:
                    max_posts = st.number_input("Max posts", min_value=1, max_value=200, value=settings.reddit_max_posts, step=1)
                with c2:
                    max_comments = st.number_input(
                        "Max comments per post",
                        min_value=1,
                        max_value=200,
                        value=settings.reddit_max_comments_per_post,
                        step=1,
                    )

                c3, c4 = st.columns(2)
                with c3:
                    sort_mode = st.selectbox("Sort mode", SORT_OPTIONS, index=SORT_OPTIONS.index(settings.default_reddit_sort_mode) if settings.default_reddit_sort_mode in SORT_OPTIONS else 0)
                with c4:
                    time_filter = st.selectbox(
                        "Time filter",
                        TIME_FILTER_OPTIONS,
                        index=TIME_FILTER_OPTIONS.index(settings.default_reddit_time_filter) if settings.default_reddit_time_filter in TIME_FILTER_OPTIONS else 0,
                        format_func=lambda value: "None" if value == "" else value,
                    )

                engine = st.selectbox(
                    "Scraper engine",
                    ["requests", "selenium"],
                    index=0 if settings.reddit_scraper_engine not in {"requests", "selenium"} else ["requests", "selenium"].index(settings.reddit_scraper_engine),
                )

                submitted = st.form_submit_button("Collect Reddit Comments", use_container_width=True)

            if submitted:
                if not event_name.strip():
                    st.error("Event name is required.")
                elif not keywords_raw.strip():
                    st.error("Keywords are required.")
                else:
                    with st.spinner("Collecting Reddit comments..."):
                        try:
                            if COLLECTOR_MODE == "lambda":
                                result = invoke_lambda_collection(
                                    event_name=event_name.strip(),
                                    keywords=keywords_raw.strip(),
                                    subreddits=subreddits_raw.strip(),
                                    max_posts=int(max_posts),
                                    max_comments_per_post=int(max_comments),
                                    sort_mode=sort_mode,
                                    time_filter=time_filter,
                                    engine=engine,
                                )
                                body = _extract_lambda_body(result)
                                if body.get("status") == "ok":
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
                                    event_name=event_name.strip(),
                                    keywords=keywords_raw.strip(),
                                    subreddits=subreddits_raw.strip(),
                                    max_posts=int(max_posts),
                                    max_comments_per_post=int(max_comments),
                                    sort_mode=sort_mode,
                                    time_filter=time_filter,
                                    engine=engine,
                                )
                                render_success(result)
                        except LambdaInvokeError as exc:
                            render_error(exc)
                        except Exception as exc:
                            st.error(f"Unexpected error while collecting Reddit data: {exc}")

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
            st.write("- The collector uses respectful delays and public Reddit pages or JSON endpoints.")
            st.write("- Spark reads Kafka, applies the BERT model, and writes processed results to Elasticsearch.")

    if auto_refresh:
        time.sleep(int(refresh_seconds))
        st.rerun()


if __name__ == "__main__":
    main()

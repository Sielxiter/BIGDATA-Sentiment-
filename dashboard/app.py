"""Streamlit dashboard focused on the serverless YouTube collector."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import settings  # noqa: E402
from dashboard.aws_lambda import LambdaInvokeError, invoke_youtube_collector_lambda  # noqa: E402


LAMBDA_FUNCTION_NAME = "youtube-collector-to-s3"
LAMBDA_REGION = "eu-north-1"


def apply_simple_style() -> None:
    st.markdown(
        """
        <style>
        .block-container {
            max-width: 1080px;
            padding-top: 1.0rem;
            padding-bottom: 1.5rem;
        }
        [data-testid="stMetricValue"] {
            font-size: 1.2rem;
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


def render_target_summary() -> None:
    st.markdown("### Target")
    st.write(f"Function: `{LAMBDA_FUNCTION_NAME}`")
    st.write(f"Region: `{LAMBDA_REGION}`")
    st.write(f"S3 prefix: `{settings.s3_prefix}`")
    st.caption("AWS credentials are read from your local `aws configure` setup through `boto3`.")


def render_flow_summary() -> None:
    st.markdown("### Flow")
    st.code(
        "Streamlit -> AWS Lambda -> S3 -> S3-to-Kafka bridge -> Kafka -> Spark -> Elasticsearch",
        language="text",
    )
    st.info(
        "This page is for manual event collection only. The Lambda writes raw JSONL files to S3, "
        "then the local S3-to-Kafka bridge forwards them into Kafka."
    )


def render_success(result: dict[str, Any]) -> None:
    decoded = result.get("decoded", {}) or {}
    body = decoded.get("body_parsed") if isinstance(decoded, dict) else None
    if not isinstance(body, dict):
        body = {}

    comments_collected = body.get("comments_collected")
    session_id = body.get("session_id")
    s3_key = body.get("s3_key")

    st.success(f"Collected {comments_collected} comments successfully.")

    c1, c2, c3 = st.columns(3)
    c1.metric("Comments", format_int(comments_collected))
    c2.metric("Session ID", str(session_id or ""))
    c3.metric("S3 Prefix", settings.s3_prefix)

    if s3_key:
        st.markdown("### S3 Path")
        st.code(s3_key, language="text")
        st.caption("This is the object key that the S3-to-Kafka bridge will consume.")

    st.warning("Next step: run `python cloud/s3_to_kafka_bridge.py --once` or keep the bridge running locally.")

    st.session_state["last_lambda_result"] = result


def render_error(exc: Exception) -> None:
    st.error(str(exc))
    st.caption("If this is a permissions issue: ensure your local IAM user has `lambda:InvokeFunction`.")
    details = getattr(exc, "details", None)
    if isinstance(details, dict) and details:
        with st.expander("Details (raw Lambda error payload)"):
            st.json(details)


def render_last_response() -> None:
    last_result = st.session_state.get("last_lambda_result")
    if not last_result:
        return

    with st.expander("Last successful Lambda response"):
        st.json(last_result)


def main() -> None:
    st.set_page_config(page_title="Serverless YouTube Collector", layout="wide", initial_sidebar_state="collapsed")
    apply_simple_style()

    st.title("Serverless YouTube Collection")
    st.caption("Launch the AWS Lambda collector directly from Streamlit.")

    left, right = st.columns([1.15, 0.85])

    with left:
        with st.container(border=True):
            st.markdown("### Collect Event Data")

            with st.form("youtube_lambda_form", clear_on_submit=False):
                event_name = st.text_input("Event name", value=settings.default_event_name)
                keywords_raw = st.text_input("Keywords (comma-separated)", value=settings.default_youtube_keywords)
                c1, c2 = st.columns(2)
                with c1:
                    max_videos = st.number_input(
                        "Max videos",
                        min_value=1,
                        max_value=10,
                        value=1,
                        step=1,
                    )
                with c2:
                    max_comments = st.number_input(
                        "Max comments",
                        min_value=1,
                        max_value=100,
                        value=20,
                        step=1,
                    )

                submitted = st.form_submit_button("Collect YouTube Comments", use_container_width=True)

            if submitted:
                if not event_name.strip():
                    st.error("Event name is required.")
                elif not keywords_raw.strip():
                    st.error("Keywords are required.")
                else:
                    with st.spinner("Invoking Lambda..."):
                        try:
                            result = invoke_youtube_collector_lambda(
                                event_name=event_name.strip(),
                                keywords=keywords_raw.strip(),
                                max_videos=int(max_videos),
                                max_comments=int(max_comments),
                                function_name=LAMBDA_FUNCTION_NAME,
                                region_name=LAMBDA_REGION,
                            )
                        except LambdaInvokeError as exc:
                            render_error(exc)
                        except Exception as exc:
                            st.error(f"Unexpected error while invoking Lambda: {exc}")
                        else:
                            render_success(result)

            render_last_response()

    with right:
        render_target_summary()
        st.divider()
        render_flow_summary()
        st.divider()
        st.markdown("### Operational Notes")
        st.write("- Streamlit uses local AWS CLI credentials.")
        st.write("- Lambda writes raw YouTube comments to S3.")
        st.write("- The S3-to-Kafka bridge should run locally after each collection.")


if __name__ == "__main__":
    main()


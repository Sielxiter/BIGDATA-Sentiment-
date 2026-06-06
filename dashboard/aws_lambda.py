"""AWS Lambda helpers for the Streamlit dashboard.

This module intentionally does not manage credentials. boto3 will use the
instance profile on EC2 or any configured AWS credentials available at runtime.
"""

from __future__ import annotations

import json
from typing import Any

import boto3
from botocore.exceptions import (
    ClientError,
    NoCredentialsError,
    PartialCredentialsError,
)

from config.settings import settings

class LambdaInvokeError(RuntimeError):
    """Raised when Lambda invocation fails in a user-actionable way."""

    def __init__(self, message: str, *, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.details = details or {}


def invoke_reddit_collector_lambda(
    *,
    event_name: str,
    keywords: str,
    subreddits: str = "",
    max_posts: int = 20,
    max_comments_per_post: int = 50,
    sort_mode: str = "relevance",
    time_filter: str = "",
    engine: str = "requests",
    function_name: str = settings.collector_lambda_function_name,
    region_name: str = settings.collector_lambda_region,
) -> dict[str, Any]:
    payload = {
        "event_name": event_name,
        "keywords": keywords,
        "subreddits": subreddits,
        "max_posts": int(max_posts),
        "max_comments_per_post": int(max_comments_per_post),
        "sort_mode": sort_mode,
        "time_filter": time_filter,
        "engine": engine,
    }

    try:
        lambda_client = boto3.client("lambda", region_name=region_name)
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload).encode("utf-8"),
        )
    except (NoCredentialsError, PartialCredentialsError) as exc:
        raise LambdaInvokeError(
            "AWS credentials not found. Attach an IAM role or run aws configure."
        ) from exc
    except ClientError as exc:
        code = (exc.response or {}).get("Error", {}).get("Code", "") if hasattr(exc, "response") else ""
        if code in {"AccessDeniedException", "UnauthorizedOperation"}:
            raise LambdaInvokeError(
                f"Permission denied. Your IAM user needs lambda:InvokeFunction for {function_name}."
            ) from exc
        raise LambdaInvokeError(f"Lambda invocation failed: {code or str(exc)}") from exc

    # Lambda can return an HTTP-style payload dict (our function does) or a raw object.
    function_error = response.get("FunctionError")
    raw_payload = response.get("Payload")
    if raw_payload is None:
        raise LambdaInvokeError(f"Lambda returned no payload. Raw response: {response}")

    try:
        body_text = raw_payload.read().decode("utf-8")
    except Exception as exc:
        raise LambdaInvokeError(f"Could not read Lambda response payload: {exc}") from exc

    try:
        decoded = json.loads(body_text) if body_text else {}
    except json.JSONDecodeError as exc:
        raise LambdaInvokeError(f"JSON parsing error from Lambda response: {exc}. Raw: {body_text[:500]}") from exc

    # Our Lambda returns: {"statusCode": 200, "body": "<json string>"}
    if isinstance(decoded, dict) and "body" in decoded and isinstance(decoded["body"], str):
        try:
            decoded_body = json.loads(decoded["body"]) if decoded["body"] else {}
        except json.JSONDecodeError:
            decoded_body = {"raw_body": decoded["body"]}
        decoded["body_parsed"] = decoded_body

    if function_error:
        raise LambdaInvokeError(
            f"Lambda execution error ({function_error}).",
            details={
                "function_error": function_error,
                "decoded_payload": decoded,
                "response_metadata": {k: v for k, v in response.items() if k != "Payload"},
            },
        )

    return {"request_payload": payload, "raw_response": response, "decoded": decoded}


def invoke_youtube_collector_lambda(**kwargs: Any) -> dict[str, Any]:
    """Compatibility alias for older code paths."""

    return invoke_reddit_collector_lambda(**kwargs)

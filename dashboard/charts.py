"""Plotly chart helpers for the Streamlit dashboard."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def _empty_figure(message: str) -> go.Figure:
    figure = go.Figure()
    figure.add_annotation(
        text=message,
        x=0.5,
        y=0.5,
        xref="paper",
        yref="paper",
        showarrow=False,
        font={"size": 16},
    )
    figure.update_layout(
        template="plotly_white",
        height=320,
        margin={"l": 30, "r": 30, "t": 60, "b": 30},
    )
    return figure


def sentiment_distribution_chart(sentiment_counts: pd.DataFrame) -> go.Figure:
    if sentiment_counts is None or sentiment_counts.empty:
        return _empty_figure("No sentiment data yet.")

    data = sentiment_counts.copy()
    if "count" not in data.columns:
        return _empty_figure("No sentiment data yet.")

    if "sentiment" not in data.columns:
        data = data.rename(columns={data.columns[0]: "sentiment"})

    return px.bar(
        data,
        x="sentiment",
        y="count",
        color="sentiment",
        text="count",
        title="Sentiment Distribution",
        color_discrete_map={
            "positive": "#2a9d8f",
            "negative": "#e76f51",
            "neutral": "#264653",
        },
    ).update_layout(template="plotly_white", height=340, showlegend=False)


def sentiment_timeline_chart(timeline_df: pd.DataFrame) -> go.Figure:
    if timeline_df is None or timeline_df.empty:
        return _empty_figure("No timeline data yet.")

    data = timeline_df.copy()
    if "timestamp" not in data.columns:
        return _empty_figure("No timeline data yet.")

    value_columns = [column for column in data.columns if column not in {"timestamp", "total"}]
    if not value_columns:
        return _empty_figure("No timeline data yet.")

    melted = data.melt(
        id_vars=["timestamp"],
        value_vars=value_columns,
        var_name="sentiment",
        value_name="count",
    )
    if melted.empty:
        return _empty_figure("No timeline data yet.")

    return px.line(
        melted,
        x="timestamp",
        y="count",
        color="sentiment",
        markers=True,
        title="Sentiment Timeline",
    ).update_layout(template="plotly_white", height=360)


def source_distribution_chart(source_df: pd.DataFrame) -> go.Figure:
    if source_df is None or source_df.empty:
        return _empty_figure("No source data yet.")

    data = source_df.copy()
    first_column = data.columns[0]
    if "count" not in data.columns:
        return _empty_figure("No source data yet.")

    return px.pie(
        data,
        names=first_column,
        values="count",
        title="Source Distribution",
        hole=0.45,
    ).update_layout(template="plotly_white", height=340)


def language_distribution_chart(language_df: pd.DataFrame) -> go.Figure:
    if language_df is None or language_df.empty:
        return _empty_figure("No language data yet.")

    data = language_df.copy()
    first_column = data.columns[0]
    if "count" not in data.columns:
        return _empty_figure("No language data yet.")

    return px.bar(
        data,
        x=first_column,
        y="count",
        color=first_column,
        text="count",
        title="Language Distribution",
    ).update_layout(template="plotly_white", height=340, showlegend=False)


def confidence_distribution_chart(posts_df: pd.DataFrame) -> go.Figure:
    if posts_df is None or posts_df.empty or "confidence" not in posts_df.columns:
        return _empty_figure("No confidence data yet.")

    data = posts_df.copy()
    data["confidence"] = pd.to_numeric(data["confidence"], errors="coerce")
    data = data.dropna(subset=["confidence"])
    if data.empty:
        return _empty_figure("No confidence data yet.")

    return px.histogram(
        data,
        x="confidence",
        nbins=20,
        title="Confidence Distribution",
        color_discrete_sequence=["#5b8def"],
    ).update_layout(template="plotly_white", height=320)


def model_version_counts_chart(version_df: pd.DataFrame) -> go.Figure:
    if version_df is None or version_df.empty:
        return _empty_figure("No model version data yet.")

    data = version_df.copy()
    if "model_version" not in data.columns or "count" not in data.columns:
        return _empty_figure("No model version data yet.")

    return px.bar(
        data,
        x="model_version",
        y="count",
        color="model_version",
        text="count",
        title="Model Version Distribution",
    ).update_layout(template="plotly_white", height=320, showlegend=False)

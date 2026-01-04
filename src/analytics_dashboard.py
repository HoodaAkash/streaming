"""Real-time analytics dashboard for streaming aggregates.

This script consumes the analytics Kafka topic emitted by ``etl_pipeline.py``
 and visualizes aggregate counts per event type. Dash + Plotly provide a
 lightweight UI without coupling to Spark. The consumer runs in a background
 thread so the UI remains responsive even when ingesting high-volume streams.
"""
from __future__ import annotations

import argparse
import json
import threading
from collections import defaultdict, deque
from datetime import datetime
from typing import Deque, Dict, Iterable, List, MutableMapping, Optional

from dash import Dash, Input, Output, dcc, html
from kafka import KafkaConsumer
import plotly.graph_objects as go


class AnalyticsBuffer:
    """Thread-safe buffer to retain the most recent aggregates per event type."""

    def __init__(self, max_points: int = 500) -> None:
        self._max_points = max_points
        self._lock = threading.Lock()
        self._data: Dict[str, Deque[dict]] = defaultdict(deque)

    def add(self, record: MutableMapping[str, object]) -> None:
        event_type = str(record.get("event_type", "unknown"))
        with self._lock:
            window = self._data[event_type]
            window.append(record)
            while len(window) > self._max_points:
                window.popleft()

    def snapshot(self) -> Dict[str, List[dict]]:
        with self._lock:
            return {k: list(v) for k, v in self._data.items()}


def consume_kafka(
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    buffer: AnalyticsBuffer,
    stop_event: threading.Event,
) -> None:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    for message in consumer:
        if stop_event.is_set():
            break
        value = message.value
        # Normalize timestamps for readable plotting
        for key in ["window_start", "window_end"]:
            if isinstance(value.get(key), str):
                try:
                    value[key] = datetime.fromisoformat(value[key])
                except ValueError:
                    pass
        buffer.add(value)

    consumer.close()


def build_figure(snapshot: Dict[str, List[dict]]) -> go.Figure:
    """Build stacked bar chart of event counts per window per type."""

    bars = []
    for event_type, rows in snapshot.items():
        x = [row.get("window_end") for row in rows]
        y = [row.get("event_count", 0) for row in rows]
        bars.append(go.Bar(name=event_type, x=x, y=y))

    fig = go.Figure(data=bars)
    fig.update_layout(
        barmode="stack",
        title="Events per window (real time)",
        xaxis_title="Window end",
        yaxis_title="Event count",
        legend_title="Event type",
        margin=dict(l=40, r=40, t=60, b=40),
    )
    return fig


def build_cards(snapshot: Dict[str, List[dict]]) -> List[html.Div]:
    """Generate summary stat cards showing the latest metrics per event type."""

    cards: List[html.Div] = []
    for event_type, rows in snapshot.items():
        latest = rows[-1]
        card = html.Div(
            className="card",
            children=[
                html.Div(className="card-title", children=event_type),
                html.Div(
                    className="card-body",
                    children=[
                        html.Div(f"Window: {latest.get('window_start')} → {latest.get('window_end')}", className="card-window"),
                        html.Div(f"Events: {latest.get('event_count', 0)}", className="card-metric"),
                        html.Div(f"Unique keys: {latest.get('unique_keys', 0)}", className="card-metric"),
                    ],
                ),
            ],
        )
        cards.append(card)
    return cards


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Real-time analytics dashboard")
    parser.add_argument("--kafka-bootstrap", required=True, help="Kafka bootstrap servers")
    parser.add_argument("--analytics-topic", required=True, help="Kafka topic with aggregates")
    parser.add_argument("--group-id", default="analytics-dashboard", help="Kafka consumer group ID")
    parser.add_argument("--max-points", type=int, default=500, help="Points retained per event type")
    parser.add_argument("--port", type=int, default=8050, help="Dashboard port")
    return parser.parse_args(argv)


def create_app(buffer: AnalyticsBuffer) -> Dash:
    app = Dash(__name__)
    app.title = "Streaming Analytics Dashboard"

    app.layout = html.Div(
        className="container",
        children=[
            html.H1("Streaming Analytics Dashboard"),
            html.Div(id="metrics-cards", className="cards"),
            dcc.Graph(id="events-graph"),
            dcc.Interval(id="refresh-interval", interval=2000, n_intervals=0),
            html.Div(
                className="footer",
                children="Powered by Dash + Kafka. Aggregates emitted from Spark Structured Streaming.",
            ),
        ],
    )

    @app.callback(
        Output("events-graph", "figure"),
        Output("metrics-cards", "children"),
        Input("refresh-interval", "n_intervals"),
    )
    def update_dashboard(_: int):
        snapshot = buffer.snapshot()
        figure = build_figure(snapshot)
        cards = build_cards(snapshot)
        return figure, cards

    return app


def main(argv: Optional[Iterable[str]] = None) -> None:
    args = parse_args(argv)
    buffer = AnalyticsBuffer(max_points=args.max_points)
    stop_event = threading.Event()

    consumer_thread = threading.Thread(
        target=consume_kafka,
        args=(
            args.kafka_bootstrap,
            args.analytics_topic,
            args.group_id,
            buffer,
            stop_event,
        ),
        daemon=True,
    )
    consumer_thread.start()

    app = create_app(buffer)

    try:
        app.run_server(host="0.0.0.0", port=args.port, debug=False)
    finally:
        stop_event.set()
        consumer_thread.join(timeout=5)


if __name__ == "__main__":
    main()

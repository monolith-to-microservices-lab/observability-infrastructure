"""Polls the Kafka Connect REST API and re-exposes connector/task state as
Prometheus gauges. Read-only against Connect's REST API - never restarts,
reconfigures, or otherwise mutates the connector.
"""
from __future__ import annotations

import logging
import os
import time

import httpx
from prometheus_client import Gauge, start_http_server

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger("connect-status-exporter")

CONNECT_URL = os.environ.get("CONNECT_URL", "http://connect:8083")
CONNECTOR_NAME = os.environ.get("CONNECTOR_NAME", "legacy-cdc-connector")
POLL_INTERVAL_SECONDS = float(os.environ.get("POLL_INTERVAL_SECONDS", "5"))
EXPORTER_PORT = int(os.environ.get("EXPORTER_PORT", "9877"))

connect_up = Gauge("kafka_connect_up", "1 if the Kafka Connect REST API responded")
connector_up = Gauge(
    "kafka_connect_connector_up", "1 if the connector state is RUNNING", ["connector"]
)
connector_failed = Gauge(
    "kafka_connect_connector_failed", "1 if the connector state is FAILED", ["connector"]
)
task_up = Gauge(
    "kafka_connect_task_up", "1 if the task state is RUNNING", ["connector", "task"]
)
task_failed = Gauge(
    "kafka_connect_task_failed", "1 if the task state is FAILED", ["connector", "task"]
)


def poll_once(client: httpx.Client) -> None:
    try:
        resp = client.get(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/status", timeout=5.0)
        resp.raise_for_status()
        connect_up.set(1)
    except httpx.HTTPError as exc:
        logger.warning("connect.unreachable", extra={"error": str(exc)})
        connect_up.set(0)
        connector_up.labels(connector=CONNECTOR_NAME).set(0)
        connector_failed.labels(connector=CONNECTOR_NAME).set(1)
        return

    body = resp.json()
    connector_state = body.get("connector", {}).get("state", "UNKNOWN")
    connector_up.labels(connector=CONNECTOR_NAME).set(1 if connector_state == "RUNNING" else 0)
    connector_failed.labels(connector=CONNECTOR_NAME).set(1 if connector_state == "FAILED" else 0)

    for task in body.get("tasks", []):
        task_id = str(task.get("id"))
        state = task.get("state", "UNKNOWN")
        task_up.labels(connector=CONNECTOR_NAME, task=task_id).set(1 if state == "RUNNING" else 0)
        task_failed.labels(connector=CONNECTOR_NAME, task=task_id).set(1 if state == "FAILED" else 0)

    logger.info(
        "connect.status_polled",
        extra={"connector_state": connector_state, "task_count": len(body.get("tasks", []))},
    )


def main() -> None:
    start_http_server(EXPORTER_PORT)
    logger.info("exporter.started", extra={"port": EXPORTER_PORT, "connect_url": CONNECT_URL})
    with httpx.Client() as client:
        while True:
            poll_once(client)
            time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()

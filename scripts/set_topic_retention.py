#!/usr/bin/env python3
"""
Set retention.ms on a list of Kafka topics.
Called from Terraform local-exec provisioners via environment variables.
Retention value is defined in scripts/common/confluent_rest.py (TOPIC_RETENTION_MS).

Environment:
  TOPICS        — space-separated topic names
  CLUSTER_ID    — Kafka cluster ID
  REST_ENDPOINT — Kafka REST Proxy endpoint
  KAFKA_KEY     — API key
  KAFKA_SECRET  — API secret
"""
import os
import sys

import requests

from scripts.common.confluent_rest import TOPIC_RETENTION_MS

_TIMEOUT = 30


def main():
    topics = os.environ["TOPICS"].split()
    cluster_id = os.environ["CLUSTER_ID"]
    rest_ep = os.environ["REST_ENDPOINT"].rstrip("/")
    key = os.environ["KAFKA_KEY"]
    secret = os.environ["KAFKA_SECRET"]

    for topic in topics:
        url = f"{rest_ep}/kafka/v3/clusters/{cluster_id}/topics/{topic}/configs:alter"
        resp = requests.post(
            url,
            auth=(key, secret),
            json={"data": [{"name": "retention.ms", "value": str(TOPIC_RETENTION_MS)}]},
            headers={"Content-Type": "application/json"},
            timeout=_TIMEOUT,
        )
        if resp.ok:
            print(f"  ✓ {topic}: retention.ms={TOPIC_RETENTION_MS} ({TOPIC_RETENTION_MS // 3_600_000}h)")
        else:
            print(f"  ⚠ {topic}: HTTP {resp.status_code} — {resp.text[:200]}", file=sys.stderr)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Submit quiz answers to the shared quiz_answers Kafka topic.

Usage:
    uv run quiz
"""

import json
import subprocess
import sys
import tempfile
from pathlib import Path

import questionary

from .common.login_checks import ensure_confluent_login, _username_from_cli
from .common.terraform import load_credentials_from_env_file, get_project_root

TOPIC = "quiz_answers"

_SCHEMA = {
    "type": "record",
    "name": "quiz_answers_value",
    "namespace": "org.apache.flink.avro.generated.record",
    "fields": [
        {"name": "participant",     "type": ["null", "string"], "default": None},
        {"name": "question_number", "type": ["null", "string"], "default": None},
        {"name": "answer",          "type": ["null", "string"], "default": None},
    ],
}


def _produce(credentials: dict, schema_file: str, participant: str, question_number: str, answer: str) -> bool:
    value = {
        "participant":     {"string": participant} if participant else None,
        "question_number": {"string": question_number},
        "answer":          {"string": answer},
    }
    cmd = [
        "confluent", "kafka", "topic", "produce", TOPIC,
        "--value-format", "avro",
        "--schema", schema_file,
        "--bootstrap", credentials["bootstrap_servers"],
        "--api-key", credentials["kafka_api_key"],
        "--api-secret", credentials["kafka_api_secret"],
        "--schema-registry-endpoint", credentials["schema_registry_url"],
        "--schema-registry-api-key", credentials["schema_registry_api_key"],
        "--schema-registry-api-secret", credentials["schema_registry_api_secret"],
    ]
    if credentials.get("environment_id"):
        cmd.extend(["--environment", credentials["environment_id"]])
    if credentials.get("cluster_id"):
        cmd.extend(["--cluster", credentials["cluster_id"]])

    result = subprocess.run(
        cmd,
        input=json.dumps(value) + "\n",
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        print(f"  ✗ Failed: {result.stderr.strip()}")
        return False
    return True


def main():
    project_root = get_project_root()
    ensure_confluent_login()

    credentials = load_credentials_from_env_file(project_root)
    if not credentials:
        print("❌ No credentials found. Run 'uv run user' first.")
        return 1

    participant = credentials.get("username") or _username_from_cli()

    schema_tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".avsc", delete=False, prefix="quiz_schema_")
    json.dump(_SCHEMA, schema_tmp)
    schema_tmp.flush()
    schema_file = schema_tmp.name

    print(f"\nQuiz — submitting as: {participant}")
    print("Enter question number and your answer. Ctrl+C to quit.\n")

    try:
        while True:
            q_num = questionary.text("Question number:").ask()
            if q_num is None:
                break
            q_num = q_num.strip()
            if not q_num:
                continue

            answer = questionary.text("Your answer:").ask()
            if answer is None:
                break
            answer = answer.strip()
            if not answer:
                continue

            if _produce(credentials, schema_file, participant, q_num, answer):
                print(f"  ✓ Answer submitted (Q{q_num})\n")

    except KeyboardInterrupt:
        print("\nDone.")
    finally:
        Path(schema_file).unlink(missing_ok=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())

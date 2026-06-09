"""
Confluent Cloud REST API helpers for creating per-user workshop resources.

Used exclusively by workshop.py (single-account mode).
Auth uses Basic auth: (api_key, api_secret).
"""

import sys
import time

import requests

CONFLUENT_API = "https://api.confluent.cloud"
_TIMEOUT = 30
_FLINK_POLL_INTERVAL = 3
_FLINK_POLL_TIMEOUT = 180


def _request(method, url, api_key, api_secret, body=None, allow_409=False):
    """Make an authenticated HTTP request. Exits on error (unless 409 and allow_409=True)."""
    resp = requests.request(
        method,
        url,
        auth=(api_key, api_secret),
        json=body,
        headers={"Content-Type": "application/json"},
        timeout=_TIMEOUT,
    )
    if allow_409 and resp.status_code == 409:
        return None  # already exists
    if not resp.ok:
        print(f"\nHTTP {resp.status_code} {method} {url}", file=sys.stderr)
        print(resp.text[:400], file=sys.stderr)
        sys.exit(1)
    return resp.json() if resp.text.strip() else {}


def cloud_api(method, path, api_key, api_secret, body=None, allow_409=False):
    return _request(method, f"{CONFLUENT_API}{path}", api_key, api_secret, body, allow_409)


def kafka_rest(method, path, rest_endpoint, kafka_key, kafka_secret, body=None, allow_409=False):
    return _request(method, f"{rest_endpoint.rstrip('/')}{path}", kafka_key, kafka_secret, body, allow_409)


def flink_rest(method, path, flink_endpoint, flink_key, flink_secret, body=None, allow_409=False):
    return _request(method, f"{flink_endpoint.rstrip('/')}{path}", flink_key, flink_secret, body, allow_409)


def get_or_create_service_account(username, api_key, api_secret):
    """Return (sa_id, api_version), creating the SA if it doesn't already exist."""
    display_name = f"{username}-workshop"

    # Page through existing service accounts looking for a match
    path = f"/iam/v2/service-accounts?page_size=100"
    while path:
        page = cloud_api("GET", path, api_key, api_secret)
        for sa in page.get("data", []):
            if sa.get("display_name") == display_name:
                print(f"  ✓ Service account already exists: {sa['id']}")
                return sa["id"], sa["api_version"]
        # Follow next page link if present
        path = page.get("metadata", {}).get("next", "")
        if path:
            # Strip domain if present (the API returns a full URL)
            path = path.replace(CONFLUENT_API, "")

    result = cloud_api("POST", "/iam/v2/service-accounts", api_key, api_secret, {
        "display_name": display_name,
        "description": f"Workshop participant service account for {username}",
    })
    print(f"  ✓ Service account created: {result['id']}")
    return result["id"], result["api_version"]


def create_api_key(display_name, description, sa_id, sa_api_version, resource_id, resource_kind, resource_api_version, env_id, api_key, api_secret):
    """Create an API key owned by the given service account. Returns (key_id, key_secret)."""
    body = {
        "spec": {
            "display_name": display_name,
            "description": description,
            "owner": {"id": sa_id, "kind": "ServiceAccount", "api_version": sa_api_version},
            "resource": {
                "id": resource_id,
                "kind": resource_kind,
                "api_version": resource_api_version,
                "environment": {"id": env_id},
            },
        }
    }
    result = cloud_api("POST", "/iam/v2/api-keys", api_key, api_secret, body)
    return result["id"], result["spec"]["secret"]


def create_role_binding(sa_id, role_name, crn_pattern, api_key, api_secret):
    """Assign a role to the service account. Silently skips if it already exists."""
    cloud_api("POST", "/iam/v2/role-bindings", api_key, api_secret, {
        "principal": f"User:{sa_id}",
        "role_name": role_name,
        "crn_pattern": crn_pattern,
    }, allow_409=True)


def create_kafka_acls(username, sa_id, cluster_id, rest_endpoint, admin_kafka_key, admin_kafka_secret):
    """Create PREFIXED ACLs allowing the user's SA to access {username}_* topics."""
    prefix = f"{username}_"
    entries = [
        ("TOPIC",   prefix,          "PREFIXED", "READ"),
        ("TOPIC",   prefix,          "PREFIXED", "WRITE"),
        ("TOPIC",   prefix,          "PREFIXED", "CREATE"),
        ("TOPIC",   prefix,          "PREFIXED", "DESCRIBE"),
        ("GROUP",   prefix,          "PREFIXED", "READ"),
        ("CLUSTER", "kafka-cluster", "LITERAL",  "DESCRIBE"),
        # Shared Lab2 source topic
        ("TOPIC",   "documents",     "LITERAL",  "READ"),
        ("TOPIC",   "documents",     "LITERAL",  "DESCRIBE"),
        # Shared Lab1 source topics — participants can query these with their own Flink key
        ("TOPIC",   "orders",        "LITERAL",  "READ"),
        ("TOPIC",   "orders",        "LITERAL",  "DESCRIBE"),
        ("TOPIC",   "products",      "LITERAL",  "READ"),
        ("TOPIC",   "products",      "LITERAL",  "DESCRIBE"),
        ("TOPIC",   "customers",     "LITERAL",  "READ"),
        ("TOPIC",   "customers",     "LITERAL",  "DESCRIBE"),
    ]
    for resource_type, resource_name, pattern_type, operation in entries:
        kafka_rest(
            "POST",
            f"/kafka/v3/clusters/{cluster_id}/acls",
            rest_endpoint,
            admin_kafka_key,
            admin_kafka_secret,
            {
                "resource_type": resource_type,
                "resource_name": resource_name,
                "pattern_type": pattern_type,
                "principal": f"User:{sa_id}",
                "host": "*",
                "operation": operation,
                "permission": "ALLOW",
            },
            allow_409=True,
        )
    print(f"  ✓ Kafka ACLs created (prefix: {prefix})")


def run_flink_statement(stmt_name, sql, org_id, env_id, pool_id, sa_id, flink_endpoint, flink_key, flink_secret, env_name, cluster_name):
    """
    Submit a Flink SQL statement and wait until it reaches RUNNING or COMPLETED.
    Silently skips if a statement with this name already exists (409).
    """
    path = f"/sql/v1/organizations/{org_id}/environments/{env_id}/statements"
    body = {
        "name": stmt_name,
        "spec": {
            "statement": sql,
            "compute_pool_id": pool_id,
            "principal": sa_id,
            "properties": {
                "sql.current-catalog": env_name,
                "sql.current-database": cluster_name,
            },
        },
    }

    result = flink_rest("POST", path, flink_endpoint, flink_key, flink_secret, body, allow_409=True)
    if result is None:
        print(f"    ✓ {stmt_name} (already exists)")
        return

    # Poll until the statement is RUNNING, COMPLETED, or FAILED
    status_path = f"{path}/{stmt_name}"
    deadline = time.time() + _FLINK_POLL_TIMEOUT
    while time.time() < deadline:
        time.sleep(_FLINK_POLL_INTERVAL)
        status = flink_rest("GET", status_path, flink_endpoint, flink_key, flink_secret)
        phase = status.get("status", {}).get("phase", "PENDING")
        if phase in ("RUNNING", "COMPLETED"):
            print(f"    ✓ {stmt_name} ({phase.lower()})")
            return
        if phase in ("FAILED", "STOPPED"):
            detail = status.get("status", {}).get("detail", "no detail")
            print(f"    ⚠ {stmt_name} {phase.lower()}: {detail}")
            return

    print(f"    ⚠ {stmt_name} timed out waiting for RUNNING/COMPLETED state")

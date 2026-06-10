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


_PERMISSION_DENIED = object()  # sentinel returned when allow_403=True and server returns 403


def _request(method, url, api_key, api_secret, body=None, allow_409=False, allow_403=False):
    """Make an authenticated HTTP request. Exits on error (unless 409/allow_409 or 403/allow_403)."""
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
    if allow_403 and resp.status_code == 403:
        return _PERMISSION_DENIED
    if not resp.ok:
        print(f"\nHTTP {resp.status_code} {method} {url}", file=sys.stderr)
        print(resp.text[:400], file=sys.stderr)
        sys.exit(1)
    return resp.json() if resp.text.strip() else {}


def cloud_api(method, path, api_key, api_secret, body=None, allow_409=False):
    return _request(method, f"{CONFLUENT_API}{path}", api_key, api_secret, body, allow_409)


def kafka_rest(method, path, rest_endpoint, kafka_key, kafka_secret, body=None, allow_409=False, allow_403=False):
    return _request(method, f"{rest_endpoint.rstrip('/')}{path}", kafka_key, kafka_secret, body, allow_409, allow_403)


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
                "environment": env_id,
            },
        }
    }
    result = cloud_api("POST", "/iam/v2/api-keys", api_key, api_secret, body)
    return result["id"], result["spec"]["secret"]


def find_user_id_by_email(email: str, api_key: str, api_secret: str) -> str | None:
    """Return the Confluent Cloud user ID (u-xxxxx) for the given email, or None."""
    path = "/iam/v2/users?page_size=100"
    while path:
        page = cloud_api("GET", path, api_key, api_secret)
        for user in page.get("data", []):
            if user.get("email") == email:
                return user["id"]
        path = page.get("metadata", {}).get("next", "")
        if path:
            path = path.replace(CONFLUENT_API, "")
    return None


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
        ("TOPIC",   "orders",                 "LITERAL",  "READ"),
        ("TOPIC",   "orders",                 "LITERAL",  "DESCRIBE"),
        ("TOPIC",   "products",               "LITERAL",  "READ"),
        ("TOPIC",   "products",               "LITERAL",  "DESCRIBE"),
        ("TOPIC",   "customers",              "LITERAL",  "READ"),
        ("TOPIC",   "customers",              "LITERAL",  "DESCRIBE"),
        # Shared Lab2 pipeline — write to queries, read the rest
        ("TOPIC",   "queries",                "LITERAL",  "READ"),
        ("TOPIC",   "queries",                "LITERAL",  "WRITE"),
        ("TOPIC",   "queries",                "LITERAL",  "DESCRIBE"),
        ("TOPIC",   "queries_embed",          "LITERAL",  "READ"),
        ("TOPIC",   "queries_embed",          "LITERAL",  "DESCRIBE"),
        ("TOPIC",   "search_results",         "LITERAL",  "READ"),
        ("TOPIC",   "search_results",         "LITERAL",  "DESCRIBE"),
        ("TOPIC",   "search_results_response","LITERAL",  "READ"),
        ("TOPIC",   "search_results_response","LITERAL",  "DESCRIBE"),
        # Shared Flink ML models — participants need READ to call ML_PREDICT
        ("MODEL",   "llm_textgen_model",      "LITERAL",  "READ"),
        ("MODEL",   "llm_embedding_model",    "LITERAL",  "READ"),
    ]

    # Confluent Cloud RBAC permissions for a newly-created Kafka key can take a few seconds
    # to propagate, so retry with backoff if the cluster returns 403.
    for attempt in range(4):
        if attempt > 0:
            wait = 5 * attempt  # 5s, 10s, 15s
            print(f"  Kafka key permissions not yet active, waiting {wait}s and retrying...")
            time.sleep(wait)

        blocked = False
        for resource_type, resource_name, pattern_type, operation in entries:
            result = kafka_rest(
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
                allow_403=True,
            )
            if result is _PERMISSION_DENIED:
                blocked = True
                break

        if not blocked:
            break
    else:
        print("\nError: Cannot create Kafka ACLs — permission denied after retries.", file=sys.stderr)
        sys.exit(1)

    print(f"  ✓ Kafka ACLs created (prefix: {prefix})")


def set_topic_retention(topic_name, cluster_id, rest_endpoint, kafka_key, kafka_secret, retention_ms=3600000):
    """Set retention.ms on a Kafka topic. Retries up to 3 times if the topic isn't visible yet."""
    path = f"/kafka/v3/clusters/{cluster_id}/topics/{topic_name}/configs:alter"
    body = {"data": [{"name": "retention.ms", "value": str(retention_ms)}]}
    url = f"{rest_endpoint.rstrip('/')}{path}"
    for attempt in range(3):
        resp = requests.request(
            "POST", url,
            auth=(kafka_key, kafka_secret),
            json=body,
            headers={"Content-Type": "application/json"},
            timeout=_TIMEOUT,
        )
        if resp.ok:
            print(f"    ✓ {topic_name}: retention set to {retention_ms // 3600000}h")
            return
        if resp.status_code == 404 and attempt < 2:
            time.sleep(5)
            continue
        print(f"    ⚠ Could not set retention on {topic_name}: HTTP {resp.status_code}", file=sys.stderr)
        return


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

#!/usr/bin/env python3
"""
Single-account workshop setup for Confluent streaming agents.

Run by each workshop PARTICIPANT after the organizer has run `uv run setup`.
Creates per-user resources (service account, API keys, ACLs) namespaced under
a prefix derived from the participant's first name.

Usage:
    uv run user            # opens browser login if session expired
    uv run user --login    # always opens browser login
"""

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

from dotenv import dotenv_values, set_key

from scripts.common.confluent_rest import (
    create_api_key,
    create_kafka_acls,
    create_role_binding,
    get_or_create_service_account,
)
from scripts.common.login_checks import confluent_login_interactive
from scripts.common.terraform import get_project_root
from scripts.common.ui import prompt_choice, prompt_with_default


# ---------------------------------------------------------------------------
# Username derivation
# ---------------------------------------------------------------------------

def _name_to_username(name: str) -> str:
    """Derive a safe, short username from a first name.

    alphanumeric + underscores, starts with a letter, max 20 chars.
    """
    clean = re.sub(r"[^a-z0-9]", "_", name.lower()).strip("_")
    if not clean or not clean[0].isalpha():
        clean = "u" + clean
    return clean[:20]


# ---------------------------------------------------------------------------
# Confluent CLI helpers
# ---------------------------------------------------------------------------

def _confluent_json(args: list) -> list | dict:
    """Run a confluent CLI command with --output json. Raises on non-zero exit."""
    result = subprocess.run(
        ["confluent"] + args + ["--output", "json"],
        capture_output=True, text=True, check=True,
    )
    return json.loads(result.stdout)


def _field(d: dict, *keys: str, default: str = "") -> str:
    """Return the first non-empty value from d matching any of keys."""
    for k in keys:
        v = d.get(k)
        if v:
            return str(v)
    return default


def _pick_from_list(items: list, label_fn) -> dict:
    """Display a numbered list and return the user-selected item."""
    for i, item in enumerate(items, 1):
        print(f"  {i}. {label_fn(item)}")
    while True:
        raw = input(f"  Enter number [1–{len(items)}]: ").strip()
        if raw.isdigit() and 1 <= int(raw) <= len(items):
            return items[int(raw) - 1]
        print(f"  Please enter a number between 1 and {len(items)}.")


def _prompt_and_cache(creds: dict, creds_file: Path, env_key: str, label: str) -> str:
    """Prompt with the cached value shown as default. Always displays the prompt.
    Saves the result back to credentials.env."""
    cached = creds.get(env_key, "").strip()
    value = prompt_with_default(label, cached)
    if value:
        set_key(str(creds_file), env_key, value)
    return value


# ---------------------------------------------------------------------------
# Interactive workshop configuration collection
# ---------------------------------------------------------------------------

def _collect_workshop_inputs(
    creds: dict,
    creds_file: Path,
    env_id: str,
    env_name: str,
    org_id: str,
) -> tuple[dict, str, str]:
    """
    Collect cluster and credential details after org/env context is already set.

    Every field is shown explicitly with its cached value as the default so
    participants can press Enter to accept or type a new value. Mirrors the
    organiser wizard experience in deploy.py.

    Returns (core_dict, confluent_cloud_api_key, confluent_cloud_api_secret).
    """
    print("\n=== Workshop Configuration ===\n")
    print(f"  Environment: {env_name} ({env_id})\n")

    # ── 1. Kafka cluster ─────────────────────────────────────────────────────
    cluster_id = cluster_name = bootstrap_ep = rest_ep = cloud = region = ""
    discovered_clusters: list = []
    try:
        discovered_clusters = _confluent_json(["kafka", "cluster", "list", "--environment", env_id])
        if not discovered_clusters:
            raise ValueError("no clusters found")
        if len(discovered_clusters) == 1:
            suggested_id = discovered_clusters[0]["id"]
        else:
            print("Select the workshop Kafka cluster:")
            picked = _pick_from_list(
                discovered_clusters,
                lambda item: (
                    f"{_field(item, 'name')} ({item['id']}) — "
                    f"{_field(item, 'cloud', 'provider')} {_field(item, 'region')}"
                ),
            )
            suggested_id = picked["id"]
    except Exception:
        suggested_id = creds.get("WORKSHOP_CLUSTER_ID", "")

    # Always show the prompt so the user can see and confirm the cluster ID.
    cluster_id = prompt_with_default(
        "Kafka Cluster ID",
        suggested_id or creds.get("WORKSHOP_CLUSTER_ID", ""),
    )
    if not cluster_id:
        print("Error: Kafka Cluster ID is required.")
        sys.exit(1)
    set_key(str(creds_file), "WORKSHOP_CLUSTER_ID", cluster_id)

    # Fetch metadata for the confirmed cluster ID.
    c = next((x for x in discovered_clusters if x["id"] == cluster_id), None)
    if c:
        cluster_name = _field(c, "name", default=cluster_id)
        bootstrap_ep = _field(c, "bootstrap_endpoint")
        rest_ep = _field(c, "rest_endpoint")
        cloud = _field(c, "cloud", "provider").lower()
        region = _field(c, "region")
    if not bootstrap_ep or not rest_ep:
        try:
            d = _confluent_json(["kafka", "cluster", "describe", cluster_id, "--environment", env_id])
            bootstrap_ep = bootstrap_ep or _field(d, "bootstrap_endpoint")
            rest_ep = rest_ep or _field(d, "rest_endpoint")
            cloud = cloud or _field(d, "cloud", "provider").lower()
            region = region or _field(d, "region")
            cluster_name = cluster_name or _field(d, "name", default=cluster_id)
        except Exception:
            pass
    cluster_use = subprocess.run(
        ["confluent", "kafka", "cluster", "use", cluster_id],
        capture_output=True, text=True,
    )
    if cluster_use.returncode != 0:
        msg = (cluster_use.stderr or cluster_use.stdout).strip()
        print(f"\nError: could not select cluster {cluster_id}: {msg}")
        sys.exit(1)
    print(f"  Cluster    : {cluster_name} ({cluster_id}) — {cloud} {region}")

    # ── 2. Schema Registry ───────────────────────────────────────────────────
    sr_id = sr_endpoint = ""
    try:
        sr = _confluent_json(["schema-registry", "cluster", "describe", "--environment", env_id])
        sr_id = _field(sr, "cluster_id", "id")
        sr_endpoint = _field(sr, "endpoint_url", "endpoint")
        print(f"  Schema Registry: {sr_id}")
    except Exception:
        pass
    if not sr_id:
        sr_id = _prompt_and_cache(creds, creds_file, "WORKSHOP_SR_ID", "Schema Registry Cluster ID")

    print()

    # ── 3. Confluent Cloud API key ────────────────────────────────────────────
    cached_cloud_key = creds.get("TF_VAR_confluent_cloud_api_key", "")
    cached_cloud_secret = creds.get("TF_VAR_confluent_cloud_api_secret", "")
    manual_option = (
        f'Enter manually [current: "{cached_cloud_key}"]' if cached_cloud_key
        else "Enter manually"
    )
    cloud_key_choice = prompt_choice(
        "Confluent Cloud API key:",
        ["Auto-create from current logged-in account", manual_option],
        default=1,
    )
    if "Auto-create" in cloud_key_choice:
        try:
            r = subprocess.run(
                ["confluent", "api-key", "create", "--resource", "cloud", "--output", "json"],
                capture_output=True, text=True, check=True,
            )
            data = json.loads(r.stdout)
            api_key = data.get("key", "")
            api_secret = data.get("secret", "")
            if not api_key or not api_secret:
                raise ValueError(f"unexpected output: {r.stdout.strip()}")
            set_key(str(creds_file), "TF_VAR_confluent_cloud_api_key", api_key)
            set_key(str(creds_file), "TF_VAR_confluent_cloud_api_secret", api_secret)
            print(f"  ✓ Cloud API Key: {api_key}")
        except Exception as exc:
            print(f"\nError: could not create Confluent Cloud API key: {exc}")
            sys.exit(1)
    else:
        api_key = prompt_with_default("Confluent Cloud API Key", cached_cloud_key)
        api_secret = prompt_with_default("Confluent Cloud API Secret", cached_cloud_secret)
        if not api_key or not api_secret:
            print("Error: Confluent Cloud API credentials are required.")
            sys.exit(1)
        set_key(str(creds_file), "TF_VAR_confluent_cloud_api_key", api_key)
        set_key(str(creds_file), "TF_VAR_confluent_cloud_api_secret", api_secret)

    # ── 4. Kafka API key ──────────────────────────────────────────────────────
    cached_kafka_key = creds.get("WORKSHOP_ADMIN_KAFKA_KEY", "")
    cached_kafka_secret = creds.get("WORKSHOP_ADMIN_KAFKA_SECRET", "")
    manual_option = (
        f'Enter manually [current: "{cached_kafka_key}"]' if cached_kafka_key
        else "Enter manually"
    )
    kafka_key_choice = prompt_choice(
        "Kafka API key:",
        ["Auto-create from current logged-in account", manual_option],
        default=1,
    )
    if "Auto-create" in kafka_key_choice:
        try:
            r = subprocess.run(
                ["confluent", "api-key", "create", "--resource", cluster_id, "--output", "json"],
                capture_output=True, text=True, check=True,
            )
            data = json.loads(r.stdout)
            admin_kk = data.get("key", "")
            admin_ks = data.get("secret", "")
            if not admin_kk or not admin_ks:
                raise ValueError(f"unexpected output: {r.stdout.strip()}")
            set_key(str(creds_file), "WORKSHOP_ADMIN_KAFKA_KEY", admin_kk)
            set_key(str(creds_file), "WORKSHOP_ADMIN_KAFKA_SECRET", admin_ks)
            print(f"  ✓ Kafka API Key: {admin_kk}")
        except Exception as exc:
            print(f"\nError: could not create Kafka API key: {exc}")
            sys.exit(1)
    else:
        admin_kk = prompt_with_default("Admin Kafka API Key", cached_kafka_key)
        admin_ks = prompt_with_default("Admin Kafka API Secret", cached_kafka_secret)
        if not admin_kk or not admin_ks:
            print("Error: Kafka API credentials are required.")
            sys.exit(1)
        set_key(str(creds_file), "WORKSHOP_ADMIN_KAFKA_KEY", admin_kk)
        set_key(str(creds_file), "WORKSHOP_ADMIN_KAFKA_SECRET", admin_ks)

    # ── 5. Big Industries MCP server (optional, for Lab 1 tool-calling) ──────
    print("\n--- Big Industries MCP server (ask your organiser, press Enter to skip) ---")
    bigind_mcp_endpoint = prompt_with_default(
        "Big Industries MCP URL",
        creds.get("TF_VAR_bigind_mcp_endpoint", ""),
    )
    bigind_mcp_token = prompt_with_default(
        "Big Industries MCP Token",
        creds.get("TF_VAR_bigind_mcp_token", ""),
    )
    if bigind_mcp_endpoint:
        set_key(str(creds_file), "TF_VAR_bigind_mcp_endpoint", bigind_mcp_endpoint)
    if bigind_mcp_token:
        set_key(str(creds_file), "TF_VAR_bigind_mcp_token", bigind_mcp_token)

    print()

    # ── Build core dict ───────────────────────────────────────────────────────
    core = {
        "confluent_environment_id":                   env_id,
        "confluent_environment_display_name":         env_name,
        "confluent_kafka_cluster_id":                 cluster_id,
        "confluent_kafka_cluster_display_name":       cluster_name,
        "confluent_kafka_cluster_bootstrap_endpoint": bootstrap_ep,
        "confluent_kafka_cluster_rest_endpoint":      rest_ep,
        "confluent_schema_registry_id":               sr_id,
        "confluent_schema_registry_rest_endpoint":    sr_endpoint,
        "confluent_organization_id":                  org_id,
        "cloud_provider":                             cloud,
        "cloud_region":                               region,
        "app_manager_kafka_api_key":                  admin_kk,
        "app_manager_kafka_api_secret":               admin_ks,
        "confluent_cloud_api_key":                    api_key,
        "confluent_cloud_api_secret":                 api_secret,
    }

    return core, api_key, api_secret


# ---------------------------------------------------------------------------
# Credentials file
# ---------------------------------------------------------------------------

def _save_user_credentials(
    root: Path,
    username: str,
    kafka_key: str,
    kafka_secret: str,
    sr_key: str,
    sr_secret: str,
    core: dict,
) -> None:
    path = root / f"{username}-credentials.env"
    lines = [
        f"# Workshop credentials for {username} — generated by uv run user",
        f"WORKSHOP_USERNAME='{username}'",
        "",
        "# Kafka",
        f"TF_VAR_kafka_api_key='{kafka_key}'",
        f"TF_VAR_kafka_api_secret='{kafka_secret}'",
        f"TF_VAR_kafka_bootstrap_endpoint='{core.get('confluent_kafka_cluster_bootstrap_endpoint', '')}'",
        f"TF_VAR_kafka_rest_endpoint='{core.get('confluent_kafka_cluster_rest_endpoint', '')}'",
        "",
        "# Schema Registry",
        f"TF_VAR_schema_registry_api_key='{sr_key}'",
        f"TF_VAR_schema_registry_api_secret='{sr_secret}'",
        f"TF_VAR_schema_registry_rest_endpoint='{core.get('confluent_schema_registry_rest_endpoint', '')}'",
        "",
        "# Shared environment",
        f"TF_VAR_environment_id='{core.get('confluent_environment_id', '')}'",
        f"TF_VAR_cluster_id='{core.get('confluent_kafka_cluster_id', '')}'",
        f"TF_VAR_topic_prefix='{username}_'",
    ]
    path.write_text("\n".join(lines) + "\n")
    print(f"\n  ✓ Credentials saved to {path.name}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Workshop participant setup")
    parser.add_argument(
        "--login",
        action="store_true",
        help="Force a fresh Confluent login even if already authenticated.",
    )
    args = parser.parse_args()

    print("=== Workshop Setup (single-account mode) ===\n")

    root = get_project_root()
    creds_file = root / "credentials.env"
    creds = dotenv_values(str(creds_file)) if creds_file.exists() else {}

    # --- 1. Pre-login inputs (no connection needed) ---
    # Collect name and workshop connection details before opening the browser
    # so we can switch to the correct org/env immediately after login.
    print("--- Participant details ---")
    first_name = prompt_with_default("Your first name", creds.get("WORKSHOP_FIRST_NAME", ""))
    if not first_name:
        print("Error: a first name is required.")
        sys.exit(1)
    set_key(str(creds_file), "WORKSHOP_FIRST_NAME", first_name)
    username = _name_to_username(first_name)
    print(f"  Username prefix: {username}_\n")

    print("--- Workshop connection details (ask your organiser) ---")
    org_id = prompt_with_default(
        "Confluent Organisation ID",
        creds.get("WORKSHOP_ORG_ID", ""),
    )
    env_id = prompt_with_default(
        "Confluent Environment ID",
        creds.get("WORKSHOP_ENV_ID", ""),
    )
    if not org_id or not env_id:
        print("Error: Organisation ID and Environment ID are required.")
        sys.exit(1)
    set_key(str(creds_file), "WORKSHOP_ORG_ID", org_id)
    set_key(str(creds_file), "WORKSHOP_ENV_ID", env_id)
    print()

    # --- 2. Confluent login ---
    # Pass org_id so the browser login lands directly in the workshop organisation.
    confluent_login_interactive(force=args.login, org_id=org_id)
    print()

    # --- 3. Switch to the workshop env ---
    env_result = subprocess.run(
        ["confluent", "environment", "use", env_id],
        capture_output=True, text=True,
    )
    if env_result.returncode != 0:
        msg = (env_result.stderr or env_result.stdout).strip()
        print(f"\nError: could not switch to environment {env_id}.")
        print(f"  {msg}")
        print(
            "\nMake sure the Organisation ID and Environment ID above are correct "
            "and that your Confluent account has access to that environment.\n"
            "Re-run with --login to open a fresh browser session."
        )
        sys.exit(1)

    # Resolve env display name now that context is correct
    env_name = env_id
    try:
        env_desc = _confluent_json(["environment", "describe", env_id])
        env_name = _field(env_desc, "name", "display_name", default=env_id)
    except Exception:
        pass

    # --- 4. Collect remaining workshop details (cluster, SR, credentials) ---
    core, api_key, api_secret = _collect_workshop_inputs(
        creds, creds_file, env_id=env_id, env_name=env_name, org_id=org_id,
    )

    cluster_id = core["confluent_kafka_cluster_id"]
    sr_id      = core["confluent_schema_registry_id"]
    rest_ep    = core["confluent_kafka_cluster_rest_endpoint"]
    admin_kk   = core["app_manager_kafka_api_key"]
    admin_ks   = core["app_manager_kafka_api_secret"]

    # --- 5. Create per-user resources ---
    print(f"\n=== Provisioning resources for {username} ===\n")

    print("Creating service account...")
    sa_id, sa_api_version = get_or_create_service_account(username, api_key, api_secret)

    org_crn = f"crn://confluent.cloud/organization={org_id}/environment={env_id}"
    create_role_binding(sa_id, "FlinkDeveloper", org_crn, api_key, api_secret)
    print("  ✓ FlinkDeveloper role assigned")

    sr_crn = f"crn://confluent.cloud/organization={org_id}/environment={env_id}/schema-registry={sr_id}"
    create_role_binding(sa_id, "DeveloperWrite", sr_crn, api_key, api_secret)
    print("  ✓ DeveloperWrite role assigned on Schema Registry")

    print("Creating Kafka API key...")
    kafka_key, kafka_secret = create_api_key(
        display_name=f"{username}-kafka-key",
        description=f"Kafka API key for workshop participant {username}",
        sa_id=sa_id, sa_api_version=sa_api_version,
        resource_id=cluster_id, resource_kind="Cluster", resource_api_version="cmk/v2",
        env_id=env_id, api_key=api_key, api_secret=api_secret,
    )
    print(f"  ✓ Kafka API key: {kafka_key}")

    print("Creating Schema Registry API key...")
    sr_key, sr_secret = create_api_key(
        display_name=f"{username}-sr-key",
        description=f"Schema Registry API key for workshop participant {username}",
        sa_id=sa_id, sa_api_version=sa_api_version,
        resource_id=sr_id, resource_kind="SchemaRegistryCluster", resource_api_version="srcm/v3",
        env_id=env_id, api_key=api_key, api_secret=api_secret,
    )
    print(f"  ✓ Schema Registry API key: {sr_key}")

    print("Creating Kafka ACLs...")
    create_kafka_acls(username, sa_id, cluster_id, rest_ep, admin_kk, admin_ks)

    # --- 6. Lab access summary ---
    print("\nLab access:")
    print("  Lab 1: shared source topics available (orders, products, customers)")
    print("  Lab 2: shared pipeline — write to 'queries', observe 'search_results_response'")

    # --- 7. Save user credentials ---
    _save_user_credentials(root, username, kafka_key, kafka_secret, sr_key, sr_secret, core)

    # --- 8. Set workshop profile so uv run publish-queries targets the right topic ---
    set_key(str(creds_file), "WORKSHOP_USERNAME", username)
    print(f"  ✓ WORKSHOP_USERNAME={username} written to credentials.env")

    print(f"\n{'=' * 50}")
    print(f"✓ Workshop setup complete for {username}")
    print(f"  Kafka API key    : {kafka_key}")
    print(f"  Credentials file : {username}-credentials.env")


if __name__ == "__main__":
    main()

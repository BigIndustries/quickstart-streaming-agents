#!/usr/bin/env python3
"""
Single-account workshop setup for Confluent streaming agents.

Run by each workshop PARTICIPANT after the organizer has run `uv run setup`.
Creates per-user resources (service account, API keys, ACLs) namespaced under
a prefix derived from the participant's first name.

Usage:
    uv run participate            # opens browser login if session expired
    uv run participate --login    # always opens browser login
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
from scripts.common.ui import prompt_choice


# ---------------------------------------------------------------------------
# Username derivation
# ---------------------------------------------------------------------------

def _name_to_username(name: str) -> str:
    """Derive a safe, short username from a first name.

    Applies the same guardrails as the old email-based derivation:
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


def _cached_prompt(creds: dict, creds_file: Path, env_key: str, label: str) -> str:
    """Return a cached value from credentials.env, or prompt and cache it."""
    cached = creds.get(env_key, "").strip()
    if cached:
        print(f"  {label}: {cached}  (cached)")
        return cached
    value = input(f"  {label}: ").strip()
    if value:
        set_key(str(creds_file), env_key, value)
    return value


# ---------------------------------------------------------------------------
# Interactive workshop configuration collection
# ---------------------------------------------------------------------------

def _collect_workshop_inputs(creds: dict, creds_file: Path) -> tuple[dict, str, str]:
    """
    Interactively collect all organizer-provisioned details needed to participate.

    Uses the Confluent CLI to auto-discover environment, cluster, and SR details
    where possible. Only admin credentials must be typed manually.

    Returns (core_dict, confluent_cloud_api_key, confluent_cloud_api_secret).
    """
    print("\n=== Workshop Configuration ===")
    print("The values below come from your workshop organiser.\n")

    # ── 1. Environment ───────────────────────────────────────────────────────
    env_id = env_name = ""
    try:
        envs = _confluent_json(["environment", "list"])
        if not envs:
            raise ValueError("no accessible environments")
        if len(envs) == 1:
            env = envs[0]
            env_id = env["id"]
            env_name = _field(env, "name", "display_name", default=env_id)
            print(f"  Environment: {env_name} ({env_id})")
        else:
            print("Select the workshop environment:")
            env = _pick_from_list(
                envs,
                lambda e: f"{_field(e, 'name', 'display_name')} ({e['id']})",
            )
            env_id = env["id"]
            env_name = _field(env, "name", "display_name", default=env_id)
    except Exception:
        env_id = _cached_prompt(creds, creds_file, "WORKSHOP_ENV_ID", "Confluent Environment ID")
        env_name = env_id

    set_key(str(creds_file), "WORKSHOP_ENV_ID", env_id)
    subprocess.run(
        ["confluent", "environment", "use", env_id],
        check=True, capture_output=True,
    )

    # ── 2. Kafka cluster ─────────────────────────────────────────────────────
    cluster_id = cluster_name = bootstrap_ep = rest_ep = cloud = region = ""
    try:
        clusters = _confluent_json(["kafka", "cluster", "list", "--environment", env_id])
        if not clusters:
            raise ValueError("no clusters found")
        if len(clusters) == 1:
            c = clusters[0]
        else:
            print("\nSelect the workshop Kafka cluster:")
            c = _pick_from_list(
                clusters,
                lambda item: (
                    f"{_field(item, 'name')} ({item['id']}) — "
                    f"{_field(item, 'cloud', 'provider')} {_field(item, 'region')}"
                ),
            )
        cluster_id = c["id"]
        cluster_name = _field(c, "name", default=cluster_id)
        bootstrap_ep = _field(c, "bootstrap_endpoint")
        rest_ep = _field(c, "rest_endpoint")
        cloud = _field(c, "cloud", "provider").lower()
        region = _field(c, "region")

        # Describe for any fields missing from the list output
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
    except Exception:
        cluster_id = _cached_prompt(creds, creds_file, "WORKSHOP_CLUSTER_ID", "Kafka Cluster ID")
        cluster_name = cluster_id

    set_key(str(creds_file), "WORKSHOP_CLUSTER_ID", cluster_id)
    subprocess.run(
        ["confluent", "kafka", "cluster", "use", cluster_id, "--environment", env_id],
        check=True, capture_output=True,
    )
    print(f"  Cluster    : {cluster_name} ({cluster_id}) — {cloud} {region}")

    # ── 3. Schema Registry ───────────────────────────────────────────────────
    sr_id = sr_endpoint = ""
    try:
        sr = _confluent_json(["schema-registry", "cluster", "describe", "--environment", env_id])
        sr_id = _field(sr, "cluster_id", "id")
        sr_endpoint = _field(sr, "endpoint_url", "endpoint")
        print(f"  Schema Registry: {sr_id}")
    except Exception:
        pass  # SR details optional; role binding creation may fail without sr_id

    # ── 4. Organisation ID ───────────────────────────────────────────────────
    org_id = ""
    # Try: confluent organization list
    for cmd in [["organization", "list"], ["iam", "organization", "list"]]:
        try:
            orgs = _confluent_json(cmd)
            if orgs:
                org_id = _field(orgs[0], "id")
                break
        except Exception:
            continue
    # Fallback: parse org from the environment CRN
    if not org_id:
        try:
            env_desc = _confluent_json(["environment", "describe", env_id])
            crn = _field(env_desc, "resource_name")
            if "organization=" in crn:
                org_id = crn.split("organization=")[1].split("/")[0]
        except Exception:
            pass
    if not org_id:
        org_id = _cached_prompt(creds, creds_file, "WORKSHOP_ORG_ID", "Confluent Organisation ID")

    print()

    # ── 6. Confluent Cloud API credentials (SA + role-binding creation) ──────
    api_key = creds.get("TF_VAR_confluent_cloud_api_key", "").strip()
    api_secret = creds.get("TF_VAR_confluent_cloud_api_secret", "").strip()
    if not api_key or not api_secret:
        print("Confluent Cloud API key (ask your organiser — used to create your service account):")
        api_key = input("  API Key   : ").strip()
        api_secret = input("  API Secret: ").strip()
        if not api_key or not api_secret:
            print("Error: Confluent Cloud API credentials are required.")
            sys.exit(1)
        set_key(str(creds_file), "TF_VAR_confluent_cloud_api_key", api_key)
        set_key(str(creds_file), "TF_VAR_confluent_cloud_api_secret", api_secret)
    else:
        print(f"  Confluent Cloud API key: {api_key[:8]}...  (cached)")
    print()

    # ── 7. Admin Kafka credentials (for ACL creation) ────────────────────────
    print("Admin Kafka API key (ask your organiser — used for ACL setup):")
    admin_kk = input("  Kafka API Key   : ").strip()
    admin_ks = input("  Kafka API Secret: ").strip()
    if not admin_kk or not admin_ks:
        print("Error: Admin Kafka credentials are required.")
        sys.exit(1)
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
        f"# Workshop credentials for {username} — generated by uv run participate",
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

    # --- 1. Confluent login ---
    confluent_login_interactive(force=args.login)
    print()

    # --- 2. Participant name → username ---
    first_name = input("Your first name: ").strip()
    if not first_name:
        print("Error: a first name is required.")
        sys.exit(1)
    username = _name_to_username(first_name)
    print(f"  Username prefix: {username}_\n")

    # --- 3. Collect all organizer-provisioned workshop details interactively ---
    core, api_key, api_secret = _collect_workshop_inputs(creds, creds_file)

    env_id       = core["confluent_environment_id"]
    cluster_id   = core["confluent_kafka_cluster_id"]
    sr_id        = core["confluent_schema_registry_id"]
    org_id       = core["confluent_organization_id"]
    rest_ep      = core["confluent_kafka_cluster_rest_endpoint"]
    admin_kk     = core["app_manager_kafka_api_key"]
    admin_ks     = core["app_manager_kafka_api_secret"]

    # --- 4. Lab selection ---
    lab_choice = prompt_choice(
        "Which labs would you like to set up?",
        [
            "Lab 1: MCP Tool Calling",
            "Lab 2: Vector Search / RAG",
            "Both Labs (1 and 2)",
        ],
        default=3,
    )
    if lab_choice == "Both Labs (1 and 2)":
        labs = ["lab1", "lab2"]
    elif lab_choice == "Lab 1: MCP Tool Calling":
        labs = ["lab1"]
    else:
        labs = ["lab2"]

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
    for lab in labs:
        if lab == "lab1":
            print("  Lab 1: shared source topics available (orders, products, customers)")
        elif lab == "lab2":
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

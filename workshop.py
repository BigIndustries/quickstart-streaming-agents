#!/usr/bin/env python3
"""
Single-account workshop setup for Confluent streaming agents.

Run by each workshop PARTICIPANT after the organizer has run `uv run setup`.
Creates per-user resources (service account, API keys, ACLs, Flink SQL tables)
namespaced under a prefix derived from the participant's Confluent email address.

Usage:
    uv run participate
"""

import re
import sys
from pathlib import Path

from dotenv import dotenv_values, set_key

from scripts.common.confluent_rest import (
    create_api_key,
    create_kafka_acls,
    create_role_binding,
    get_or_create_service_account,
    run_flink_statement,
    set_topic_retention,
)
from scripts.common.login_checks import ensure_confluent_login
from scripts.common.terraform import get_project_root, run_terraform_output
from scripts.common.ui import prompt_choice, prompt_with_default
from scripts.mcp_setup import setup_mcp_for_outputs


# ---------------------------------------------------------------------------
# Username derivation
# ---------------------------------------------------------------------------

def _email_to_username(email: str) -> str:
    """Derive a safe, short username from an email local-part.

    Kafka topics allow underscores; Flink statement names require [a-z0-9-].
    We use underscores for topic/table prefixes and hyphens for statement names
    (see _stmt).  The prefix itself only needs to be alphanumeric + underscore.
    """
    local = email.split("@")[0]
    clean = re.sub(r"[^a-z0-9]", "_", local.lower()).strip("_")
    # Ensure the username starts with a letter so statement names are valid
    if not clean or not clean[0].isalpha():
        clean = "u" + clean
    return clean[:20]


# ---------------------------------------------------------------------------
# Core state loader
# ---------------------------------------------------------------------------

def _load_core_outputs(root: Path) -> dict:
    state_path = root / "terraform" / "core" / "terraform.tfstate"
    if not state_path.exists():
        print("Error: terraform/core/terraform.tfstate not found.")
        print("The workshop organizer must run `uv run setup` first.")
        sys.exit(1)
    return run_terraform_output(state_path)


# ---------------------------------------------------------------------------
# Per-lab Flink SQL table setup
# ---------------------------------------------------------------------------

def _stmt(username: str, suffix: str) -> str:
    """Build a globally-unique Flink statement name (only [a-z0-9-] allowed)."""
    # Replace underscores (valid in topic names) with hyphens for statement names
    safe = username.replace("_", "-")
    return f"{safe}-{suffix}"


def _setup_lab2(u, org_id, env_id, pool_id, sa_id, fep, fk, fs, env_name, cluster_name):
    """Lab 2 – Vector Search & RAG: queries, queries_embed, documents_vectordb, search pipelines."""
    print("  Creating Lab 2 tables and pipelines...")

    stmts = [
        # Input table
        (_stmt(u, "queries-table"),
         f"CREATE TABLE IF NOT EXISTS `{env_name}`.`{cluster_name}`.`{u}_queries` (query STRING NOT NULL);"),

        # Embedding sink
        (_stmt(u, "queries-embed-table"),
         f"CREATE TABLE IF NOT EXISTS `{env_name}`.`{cluster_name}`.`{u}_queries_embed` (query STRING, embedding ARRAY<FLOAT>);"),

        # MongoDB vector-store connector (references shared mongodb-connection)
        # The organizer's pipeline (documents → documents_embed → MongoDB) populates
        # this collection; participants only need a per-user lookup table pointing at it.
        (_stmt(u, "documents-vectordb-table"),
         f"CREATE TABLE IF NOT EXISTS `{u}_documents_vectordb_lab2` ("
         "document_id STRING, chunk STRING, embedding ARRAY<FLOAT>"
         ") WITH ("
         "'connector' = 'mongodb',"
         "'mongodb.connection' = 'mongodb-connection',"
         "'mongodb.database' = 'vector_search',"
         "'mongodb.collection' = 'documents',"
         "'mongodb.index' = 'vector_index',"
         "'mongodb.embedding_column' = 'embedding',"
         "'mongodb.numCandidates' = '500');"),

        # Streaming: embed queries (INSERT INTO runs continuously)
        (_stmt(u, "queries-embed-insert"),
         f"INSERT INTO `{env_name}`.`{cluster_name}`.`{u}_queries_embed` "
         f"SELECT query, embedding FROM `{env_name}`.`{cluster_name}`.`{u}_queries`, "
         "LATERAL TABLE(ML_PREDICT('llm_embedding_model', query));"),

        # Streaming: vector search results
        (_stmt(u, "search-results-table"),
         f"CREATE TABLE IF NOT EXISTS `{u}_search_results` AS "
         f"SELECT qe.query,"
         "vs.search_results[1].document_id AS document_id_1, vs.search_results[1].chunk AS chunk_1, vs.search_results[1].score AS score_1,"
         "vs.search_results[2].document_id AS document_id_2, vs.search_results[2].chunk AS chunk_2, vs.search_results[2].score AS score_2,"
         "vs.search_results[3].document_id AS document_id_3, vs.search_results[3].chunk AS chunk_3, vs.search_results[3].score AS score_3 "
         f"FROM `{env_name}`.`{cluster_name}`.`{u}_queries_embed` AS qe, "
         f"LATERAL TABLE(VECTOR_SEARCH_AGG(`{u}_documents_vectordb_lab2`, DESCRIPTOR(embedding), qe.embedding, 3)) AS vs;"),

        # Streaming: RAG response generation
        (_stmt(u, "search-results-response-table"),
         f"CREATE TABLE IF NOT EXISTS `{u}_search_results_response` AS "
         f"SELECT sr.query, sr.document_id_1, sr.chunk_1, sr.score_1,"
         "sr.document_id_2, sr.chunk_2, sr.score_2, sr.document_id_3, sr.chunk_3, sr.score_3,"
         "pred.response "
         f"FROM `{u}_search_results` sr, "
         "LATERAL TABLE(ml_predict('llm_textgen_model',"
         f"CONCAT('Based on the following search results, answer the user query.\\n\\nUSER QUERY: ', sr.query,"
         "'\\n\\nDocument 1: ', sr.chunk_1, '\\n\\nDocument 2: ', sr.chunk_2,"
         "'\\n\\nDocument 3: ', sr.chunk_3, '\\n\\nRESPONSE:'))) AS pred;"),
    ]
    for name, sql in stmts:
        run_flink_statement(name, sql, org_id, env_id, pool_id, sa_id, fep, fk, fs, env_name, cluster_name)



# ---------------------------------------------------------------------------
# Credentials file
# ---------------------------------------------------------------------------

def _save_user_credentials(root: Path, username: str, email: str, kafka_key: str, kafka_secret: str, sr_key: str, sr_secret: str, core: dict) -> None:
    path = root / f"{username}-credentials.env"
    lines = [
        f"# Workshop credentials for {email} — generated by uv run participate",
        f"WORKSHOP_USERNAME='{username}'",
        f"WORKSHOP_EMAIL='{email}'",
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
    print("=== Workshop Setup (single-account mode) ===\n")

    root = get_project_root()
    creds_file = root / "credentials.env"
    creds = dotenv_values(str(creds_file)) if creds_file.exists() else {}

    # --- 1. Confluent login ---
    ensure_confluent_login(creds)
    print("✓ Confluent CLI logged in\n")

    # --- 2. Participant email → username ---
    email = creds.get("CONFLUENT_EMAIL", "").strip()
    if not email:
        email = input("Your Confluent Cloud email address: ").strip()
        if not email or "@" not in email:
            print("Error: a valid email address is required.")
            sys.exit(1)

    username = _email_to_username(email)
    print(f"Participant username : {username}")
    print(f"Topic/table prefix  : {username}_\n")

    # --- 3. Read shared infrastructure from core Terraform state ---
    print("Reading shared infrastructure from organizer's deployment...")
    core = _load_core_outputs(root)

    env_id        = core["confluent_environment_id"]
    cluster_id    = core["confluent_kafka_cluster_id"]
    sr_id         = core["confluent_schema_registry_id"]
    org_id        = core["confluent_organization_id"]
    pool_id       = core["confluent_flink_compute_pool_id"]
    flink_ep      = core["confluent_flink_rest_endpoint"]
    rest_ep       = core["confluent_kafka_cluster_rest_endpoint"]
    env_name      = core["confluent_environment_display_name"]
    cluster_name  = core["confluent_kafka_cluster_display_name"]
    cloud         = core["cloud_provider"]
    cloud_region  = core["cloud_region"]
    admin_kk      = core["app_manager_kafka_api_key"]
    admin_ks      = core["app_manager_kafka_api_secret"]
    admin_fk      = core["app_manager_flink_api_key"]
    admin_fs      = core["app_manager_flink_api_secret"]

    print(f"  Environment : {env_name} ({env_id})")
    print(f"  Cluster     : {cluster_name} ({cluster_id})")
    print(f"  Cloud       : {cloud} / {cloud_region}\n")

    # --- 4. Confluent Cloud API credentials (for SA / API-key creation) ---
    api_key    = creds.get("TF_VAR_confluent_cloud_api_key", "").strip()
    api_secret = creds.get("TF_VAR_confluent_cloud_api_secret", "").strip()
    if not api_key or not api_secret:
        print("Confluent Cloud API credentials needed to create per-user resources.")
        print("  (These are the admin API key/secret, not your personal password.)\n")
        api_key    = input("  Confluent Cloud API Key   : ").strip()
        api_secret = input("  Confluent Cloud API Secret: ").strip()
        if not api_key or not api_secret:
            print("Error: Confluent Cloud API credentials are required.")
            sys.exit(1)

    # --- 5. Lab selection ---
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

    # --- 6. Create per-user resources ---
    print(f"\n=== Provisioning resources for {username} ===\n")

    # Service account
    print("Creating service account...")
    sa_id, sa_api_version = get_or_create_service_account(username, api_key, api_secret)

    # FlinkDeveloper role so the SA can execute Flink SQL
    org_crn = f"crn://confluent.cloud/organization={org_id}/environment={env_id}"
    create_role_binding(sa_id, "FlinkDeveloper", org_crn, api_key, api_secret)
    print(f"  ✓ FlinkDeveloper role assigned")

    # DeveloperWrite on Schema Registry so the SA can read and register schemas
    # for all shared topics (queries, search_results, etc.)
    sr_crn = f"crn://confluent.cloud/organization={org_id}/environment={env_id}/schema-registry={sr_id}"
    create_role_binding(sa_id, "DeveloperWrite", sr_crn, api_key, api_secret)
    print(f"  ✓ DeveloperWrite role assigned on Schema Registry")

    # Kafka API key
    print("Creating Kafka API key...")
    kafka_key, kafka_secret = create_api_key(
        display_name=f"{username}-kafka-key",
        description=f"Kafka API key for workshop participant {username}",
        sa_id=sa_id, sa_api_version=sa_api_version,
        resource_id=cluster_id, resource_kind="Cluster", resource_api_version="cmk/v2",
        env_id=env_id, api_key=api_key, api_secret=api_secret,
    )
    print(f"  ✓ Kafka API key: {kafka_key}")

    # Schema Registry API key
    print("Creating Schema Registry API key...")
    sr_key, sr_secret = create_api_key(
        display_name=f"{username}-sr-key",
        description=f"Schema Registry API key for workshop participant {username}",
        sa_id=sa_id, sa_api_version=sa_api_version,
        resource_id=sr_id, resource_kind="SchemaRegistryCluster", resource_api_version="srcm/v3",
        env_id=env_id, api_key=api_key, api_secret=api_secret,
    )
    print(f"  ✓ Schema Registry API key: {sr_key}")

    # Kafka ACLs (prefix-based on {username}_)
    print("Creating Kafka ACLs...")
    create_kafka_acls(username, sa_id, cluster_id, rest_ep, admin_kk, admin_ks)

    # --- 7. Per-lab Flink SQL tables ---
    # We use the ADMIN Flink key to submit statements (to avoid bootstrap-permission issues),
    # but set the statement principal to the user's SA so the Flink job runs under their ACLs.
    print("\nCreating per-user Flink SQL tables...")
    for lab in labs:
        if lab == "lab1":
            # Lab 1 uses shared source topics (orders, products, customers) that the
            # organizer populates via datagen. Participants have READ ACLs on those topics.
            # MCP is configured with organizer credentials — no per-user tables needed.
            print("  Lab 1: shared source topics available (orders, products, customers)")
        elif lab == "lab2":
            _setup_lab2(username, org_id, env_id, pool_id, sa_id, flink_ep, admin_fk, admin_fs, env_name, cluster_name)

    # --- 8. Set 1-hour retention on per-user Kafka topics ---
    if "lab2" in labs:
        print("\nSetting topic retention (1 hour)...")
        for topic in [
            f"{username}_queries",
            f"{username}_queries_embed",
            f"{username}_search_results",
            f"{username}_search_results_response",
        ]:
            set_topic_retention(topic, cluster_id, rest_ep, admin_kk, admin_ks)

    # --- 9. Save user credentials ---
    _save_user_credentials(root, username, email, kafka_key, kafka_secret, sr_key, sr_secret, core)

    # --- 10. Configure MCP using the organizer's shared credentials ---
    print("\nConfiguring MCP server...")
    setup_mcp_for_outputs(core, root)

    # --- 11. Set workshop profile so uv run publish-queries targets the right topic ---
    set_key(str(creds_file), "WORKSHOP_USERNAME", username)
    print(f"  ✓ WORKSHOP_USERNAME={username} written to credentials.env")

    print(f"\n{'=' * 50}")
    print(f"✓ Workshop setup complete for {username}")
    print(f"  Topic / table prefix: {username}_")
    print(f"  Kafka API key       : {kafka_key}")
    print(f"  Credentials file    : {username}-credentials.env")
    print(f"  Restart Claude Code to activate the MCP server.")


if __name__ == "__main__":
    main()

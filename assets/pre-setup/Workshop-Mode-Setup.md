# Workshop Mode Setup Guide

This guide describes the **single-account workshop model**: the organizer provisions one shared Confluent Cloud environment once, and each participant runs `uv run user` to create their own namespaced resources within it.

## How it works

| Role | Command | When |
|------|---------|------|
| **Organizer** | `uv run setup` | Once, before the workshop starts |
| **Each participant** | `uv run user` | During the workshop |

The organizer's `uv run setup` creates the shared Confluent environment, cluster, LLM connections, models, and source data. Participants cannot start until this is done. Each `uv run user` then creates a personal service account, API keys, and Flink tables namespaced under the participant's username (derived from their Confluent Cloud email).

---

## Organizer Checklist

### Step 1 — Obtain LLM credentials

Participants share the organizer's LLM credentials. Provision them once before the workshop.

**AWS Bedrock (recommended):**

```bash
# Create a dedicated IAM user with Bedrock-only permissions
uv run api-keys create

# After workshop - immediately revoke
uv run api-keys destroy
```

This creates an IAM user with minimal Bedrock permissions and writes the keys to `credentials.env`. Alternatively, create the IAM user manually in the AWS Console with this policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"],
    "Resource": "*"
  }]
}
```

> [!WARNING]
> To access Claude Sonnet 4.5 you must request model access first. Visit the [AWS Bedrock Model Catalog](https://console.aws.amazon.com/bedrock/home#/model-catalog), select **Claude Sonnet 4.5**, open it in the Playground, and send a message — the access request form appears automatically. Do this well before the workshop.

**Azure OpenAI:**

```bash
uv run api-keys create
uv run api-keys destroy   # after workshop
```

This creates an Azure Cognitive Services account with `gpt-5-mini` and `text-embedding-ada-002` deployments. Requires Azure CLI (`az login`) and an active subscription.

---

### Step 2 — Run `uv run setup`

```bash
uv run setup
```

The wizard will ask you to choose:
- Cloud provider (AWS or Azure)
- LLM credentials
- Which labs to deploy (Lab 1, Lab 2, or both)
- Remote MCP backend (Lab 1 only — choose **Confluent-hosted** unless you have a reason not to)
- MongoDB connection (Lab 2 only — leave blank to use the pre-configured workshop defaults)

`uv run setup` creates the shared resources that all participants will use:

| Resource | Purpose |
|----------|---------|
| Confluent Environment + Kafka Cluster | Shared event backbone |
| Flink Compute Pool | Shared processing for all Flink SQL |
| LLM connection + models | `llm_textgen_model`, `llm_embedding_model` |
| **Lab 1**: MCP connection, `remote_mcp_model`, `orders`/`products`/`customers` topics | Source data for price matching |
| **Lab 2**: MongoDB connection, `documents` topic, `documents_embed` pipeline + MongoDB Sink | Document ingestion and vector store |

This runs once. If you need to deploy additional labs later, re-run `uv run setup` — Terraform will only apply the missing resources.

---

### Step 3 — Generate source data

**Lab 1 — start the data generator:**

```bash
uv run lab1-datagen
```

Keep this running throughout the workshop. It produces one order every 2 minutes into the shared `orders`, `products`, and `customers` topics that all participants can read.

**Lab 2 — publish documents to the knowledge base:**

```bash
uv run publish-docs --lab2
```

This pushes Flink documentation chunks into the `documents` Kafka topic. The organizer's shared embedding pipeline (`documents` → `documents_embed` → MongoDB) will process them automatically. Participants' vector search queries will work once this pipeline has processed at least one batch of documents (allow ~2 minutes).

---

### Step 4 — Invite participants to the Confluent Cloud organization

Each participant needs a Confluent Cloud account **in the same organization** as the organizer. Invite them via the Confluent Cloud UI: **Organization → IAM → Invite users**.

Participants only need the **MetricsViewer** role at the organization level — `uv run user` creates all the permissions they need.

---

### Step 5 — Share the Terraform state file with participants

Participants need the organizer's `terraform/core/terraform.tfstate` to discover the shared environment. Distribute it securely (e.g., a shared network drive, a secure Slack DM, or include it in a pre-configured repo fork). Participants place it at `terraform/core/terraform.tfstate` in their local clone.

> [!NOTE]
> This file contains API credentials. Do not commit it to a public repository or share it over insecure channels.

---

## Participant Checklist

### Step 1 — Clone the repository

```bash
git clone https://github.com/BigIndustries/quickstart-streaming-agents.git
cd quickstart-streaming-agents
```

### Step 2 — Place the shared state file

Get `terraform/core/terraform.tfstate` from the organizer and place it at:

```
quickstart-streaming-agents/terraform/core/terraform.tfstate
```

### Step 3 — Log in to Confluent Cloud

```bash
confluent login
```

Use the Confluent Cloud account the organizer invited you to.

### Step 4 — Run `uv run user`

```bash
uv run user
```

The script will:
1. Derive your username from your Confluent Cloud email (e.g. `alice@example.com` → prefix `alice`)
2. Create a personal service account `workshop-alice`
3. Create Kafka, Schema Registry, and Flink API keys
4. Grant you READ access to the shared source topics (`orders`, `products`, `customers`, `documents`)
5. Create your personal namespaced Flink tables (e.g. `alice_queries`, `alice_search_results`, etc.)
6. Configure the MCP server with the organizer's shared credentials

At the end, your credentials are saved to `credentials-alice.env`.

### Step 5 — Follow the lab walkthrough

- **Lab 1**: [LAB1-Walkthrough.md](../../LAB1-Walkthrough.md) — when the walkthrough shows table names like `orders`, those are the shared source topics you can read directly.
- **Lab 2**: [LAB2-Walkthrough.md](../../LAB2-Walkthrough.md) — when the walkthrough shows table names like `queries` or `search_results`, substitute your prefix (e.g. `alice_queries`, `alice_search_results`).

---

## After the Workshop

**Organizer:**

```bash
# Destroy all shared Confluent infrastructure
uv run destroy

# Revoke LLM credentials
uv run api-keys destroy
```

**Participants:**

```bash
# Remove local credential files
rm credentials*.env
```

---

## Security Notes

- Generate LLM credentials the day before the workshop; revoke them immediately after.
- Do not reuse the same API keys across workshops.
- Distribute `terraform/core/terraform.tfstate` via secure channels only — it contains Kafka and Flink API keys.
- Participants: never commit `credentials.env` or `terraform.tfstate` to Git.

---

## Presenter Tips

**Before the workshop:**
- Enable Bedrock model access at least 24 hours in advance — approval can take time.
- Run `uv run setup` the evening before to verify everything deploys cleanly.
- Run `uv run lab1-datagen` for a few minutes to pre-populate the `orders` topic.
- For Lab 2, run `uv run publish-docs --lab2` and confirm documents appear in MongoDB before participants arrive.

**During the workshop:**
- Remind participants their table prefix comes from their Confluent email local part (e.g. `john.doe@example.com` → `john_doe`).
- The `uv run user` output shows the exact prefix and a summary of what was created.
- For Lab 2, remind participants to substitute their prefix in every SQL query.
- Remind Lab 1 participants to add their email address to the price-matching query before running it.

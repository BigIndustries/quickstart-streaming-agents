# Lab2: Vector Search / RAG Walkthrough

In this lab, we'll create a Retrieval-Augmented Generation (RAG) pipeline using Confluent Cloud for Apache Flink's vector search capabilities. The pipeline processes documents, creates embeddings, and enables semantic search to power intelligent responses through retrieval of relevant context.

<img src="./assets/lab2/00_lab2_architecture.png" alt="Lab2 Architecture Diagram"/>

## Prerequisites

> [!NOTE]
>
> These prerequisites are not required in instructor-led workshops — credentials will be provided for you.

- **LLM Access:** AWS Bedrock API keys **OR** Azure OpenAI endpoint + API key
- **MongoDB vector database:** Pre-configured and managed for you - no setup required.

> [!WARNING]
>
> **AWS Bedrock Users:** You must request access to Claude Sonnet 4.5 by filling out an Anthropic use case form. Visit the [Model Catalog](https://console.aws.amazon.com/bedrock/home#/model-catalog), select Claude Sonnet 4.5, open it in the Playground, and send a message - the form will appear automatically.

## Deployment

If you haven't already, clone the repo:

```bash
git clone https://github.com/BigIndustries/quickstart-streaming-agents.git
cd quickstart-streaming-agents
```

**Self-service (single user):** Run the setup wizard and select **Lab 2**:

```bash
uv run setup
```

**Workshop participant:** The organizer must have already run `uv run setup` before you proceed. Run the following command to create your personal resources:

```bash
uv run participate
```

> [!NOTE]
> In a workshop, the organizer's `uv run setup` has already deployed the shared pipeline that embeds documents into MongoDB. `uv run participate` creates your personal Flink tables for querying. The organizer must also have published documents with `uv run publish-docs --lab2` before your queries will return results.

`uv run setup` deploys the complete RAG pipeline. Resources are split between shared (organizer) and per-user (participant):

**Shared resources** (created once by `uv run setup`):

| Resource | Purpose |
|----------|---------|
| `documents` | Kafka topic — organizer publishes docs here |
| `documents_embed` | Kafka topic — Flink embeds docs from `documents` |
| MongoDB Sink Connector | Streams `documents_embed` → MongoDB Atlas vector store |
| `documents_vectordb_lab2` | Flink lookup table backed by MongoDB |
| `llm_textgen_model`, `llm_embedding_model` | Shared LLM models |

**Per-user resources** (created by `uv run participate`, prefixed with your username):

| Resource | Purpose |
|----------|---------|
| `{prefix}_queries` | Your query input topic |
| `{prefix}_queries_embed` | Your queries with embeddings |
| `{prefix}_documents_vectordb_lab2` | Your personal MongoDB lookup table |
| `{prefix}_search_results` | Your vector search results |
| `{prefix}_search_results_response` | Your RAG responses |

## Using the RAG Pipeline

### Load Confluent Flink Documentation

The lab uses real Confluent Flink documentation as the knowledge base. These pre-populated documents have been:

1. **Embedded** using an LLM embedding model
2. **Stored** in a MongoDB Atlas cluster with vector search index
3. **Made searchable** for semantic queries

### Query the RAG System

```bash
uv run publish-queries # starts interactive mode (recommended), or:
uv run publish-queries "How do window functions work in Flink SQL?"
```

Your queries land in your personal `{prefix}_queries` topic (e.g. `alice_queries`), which feeds into `{prefix}_queries_embed` where embeddings are created, and then through vector search into `{prefix}_search_results` and `{prefix}_search_results_response`.

> [!NOTE]
> **Workshop participants:** throughout this lab, substitute your personal prefix wherever you see a table name. Your prefix is shown at the end of `uv run participate` output (e.g. `alice`, `john_doe`). So `queries` becomes `alice_queries`, `search_results` becomes `alice_search_results`, and so on.

The results contain:
- Source document snippets with similarity scores
- Document ID
- AI-generated RAG response based on retrieved documents

### View Results in Confluent Cloud

Monitor the pipeline in the Confluent Cloud SQL workspace. Replace `{prefix}` with your username prefix:

```sql
-- Check data flow through your personal pipeline
SELECT
  (SELECT COUNT(*) FROM {prefix}_queries) AS queries_count,
  (SELECT COUNT(*) FROM {prefix}_queries_embed) AS queries_embed_count,
  (SELECT COUNT(*) FROM {prefix}_search_results) AS search_results_count,
  (SELECT COUNT(*) FROM {prefix}_search_results_response) AS search_results_response_count;

-- See vector search results
SELECT * FROM {prefix}_search_results LIMIT 5;

-- View final RAG responses
SELECT query, response FROM {prefix}_search_results_response LIMIT 5;
```

**Self-service users** (single deployment, no prefix):

```sql
SELECT
  (SELECT COUNT(*) FROM queries) AS queries_count,
  (SELECT COUNT(*) FROM queries_embed) AS queries_embed_count,
  (SELECT COUNT(*) FROM search_results) AS search_results_count,
  (SELECT COUNT(*) FROM search_results_response) AS search_results_response_count;

SELECT * FROM search_results LIMIT 5;
SELECT query, response FROM search_results_response LIMIT 5;
```

## Troubleshooting

<details>
<summary>Click to expand</summary>

### Common Issues

1. **Pipeline not processing**: Wait 30-60 seconds after publishing documents for initial processing
2. **No query responses**: Check that LLM models are deployed. [Run test query #1](./LAB1-Walkthrough.md#test-query-1-base-llm-model) to verify `llm_textgen_model` is working
3. **Empty results**: Verify MongoDB sink connector status in Confluent Cloud UI
4. **Deployment failed**: Ensure you have valid LLM credentials (Bedrock keys or Azure OpenAI endpoint/key)

</details>

## Navigation

- **← Back to Overview**: [Main README](./README.md)
- **← Previous Lab**: [Lab1: Tool Calling Agent](./LAB1-Walkthrough.md)
- **🧹 Cleanup**: [Cleanup Instructions](./README.md#cleanup)

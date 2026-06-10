# Lab2: Vector Search / RAG Walkthrough

In this lab, we'll create a Retrieval-Augmented Generation (RAG) pipeline using Confluent Cloud for Apache Flink's vector search capabilities. The pipeline processes documents, creates embeddings, and enables semantic search to power intelligent responses through retrieval of relevant context.

<img src="./assets/lab2/00_lab2_architecture.png" alt="Lab2 Architecture Diagram"/>

## Prerequisites

**Shared resources** (Already created by the organizer through `uv run setup`):

| Resource | Purpose |
|----------|---------|
| `documents` | Kafka topic — organizer publishes docs here |
| `documents_embed` | Kafka topic — Flink embeds docs from `documents` |
| `MongoDB Sink Connector` | Streams `documents_embed` → `MongoDB Atlas vector store` |
| `documents_vectordb_lab2` | Flink lookup table backed by MongoDB |
| `llm_textgen_model`, `llm_embedding_model` | Shared LLM models |

**Participant access** (granted by `uv run user`):

| Topic | Access | Purpose |
|-------|--------|---------|
| `queries` | Read + Write | Send your queries here |
| `queries_embed` | Read | Your query after embedding |
| `search_results` | Read | Vector search results |
| `search_results_response` | Read | Final RAG responses |

All participants share the same pipeline — queries from everyone flow through the same Flink jobs and appear in the same result topics.

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

Your queries land in the shared `queries` topic, which feeds into `queries_embed` where embeddings are created, then through vector search into `search_results` and `search_results_response`. In a workshop, queries from all participants flow through the same shared pipeline.

The results contain:
- Source document snippets with similarity scores
- Document ID
- AI-generated RAG response based on retrieved documents

### View Results in Confluent Cloud

Monitor the shared pipeline in the Confluent Cloud SQL workspace:

```sql
-- Check data flow through the pipeline
SELECT
  (SELECT COUNT(*) FROM queries) AS queries_count,
  (SELECT COUNT(*) FROM queries_embed) AS queries_embed_count,
  (SELECT COUNT(*) FROM search_results) AS search_results_count,
  (SELECT COUNT(*) FROM search_results_response) AS search_results_response_count;

-- See vector search results
SELECT * FROM search_results LIMIT 5;

-- View final RAG responses
SELECT query, response FROM search_results_response LIMIT 5;
```

## Optional: Publish Your Own Documents to VectorDB for RAG knowledgebase
<details>
<summary>Click to expand</summary>

You can extend the knowledge base with any web page. The `web2md` tool fetches a URL and saves it as Markdown; `publish-docs` then streams those files into the shared `documents` topic where they flow through embedding and vector indexing automatically.

**Step 1 — Fetch a web page as Markdown**

```bash
uv run web2md <url> assets/md/
```

Example — add the Confluent Flink SQL overview page:

```bash
uv run web2md https://docs.confluent.io/cloud/current/flink/overview.html assets/md/
```

The file is saved as `assets/md/<page-slug>.md`.

**Step 2 — Publish it to the knowledge base**

```bash
uv run publish-docs --docs-dir assets/md/
```

This publishes all Markdown files in `assets/md/` (including the one you just added) to the `documents` Kafka topic. Within a minute, the document will be embedded, indexed in MongoDB, and available for vector search for the RAG system.

**Step 3 — Query the new content**

```bash
uv run publish-queries "What is <topic from your new document>?"
```

> **Tip:** You can also publish from a different directory using `--docs-dir <path>`.

</details>

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

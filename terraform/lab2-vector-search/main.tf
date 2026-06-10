# Reference to core infrastructure
data "terraform_remote_state" "core" {
  backend = "local"
  config = {
    path = "../core/terraform.tfstate"
  }
}

# Random ID for unique resource names for this lab
resource "random_id" "lab_suffix" {
  byte_length = 4
}

# Local values
locals {
  cloud_provider = data.terraform_remote_state.core.outputs.cloud_provider
  cloud_region   = data.terraform_remote_state.core.outputs.cloud_region

  # Cloud-specific MongoDB defaults
  mongodb_defaults = {
    aws   = { conn = "mongodb+srv://cluster0.vqg04jw.mongodb.net/", user = "currentlondon2026_db_user", pass = "VyARxV0Pn7DEXaYn" }
    azure = { conn = "mongodb+srv://cluster0.vqg04jw.mongodb.net/", user = "public_readonly_user", pass = "sB948mVgIYqwUloX" }
  }

  effective_mongodb_conn = var.mongodb_connection_string != "" ? var.mongodb_connection_string : local.mongodb_defaults[local.cloud_provider].conn
  effective_mongodb_user = var.mongodb_username != "" ? var.mongodb_username : local.mongodb_defaults[local.cloud_provider].user
  effective_mongodb_pass = var.mongodb_password != "" ? var.mongodb_password : local.mongodb_defaults[local.cloud_provider].pass

  # Extract hostname from mongodb+srv://hostname
  # Strip protocol and trailing slash: "mongodb+srv://host/" -> "host"
  mongodb_host = trimsuffix(split("//", local.effective_mongodb_conn)[1], "/")
}

# ------------------------------------------------------
# UNIFIED RESOURCES FOR LAB2-VECTOR-SEARCH
# ------------------------------------------------------

# Lab2 uses the shared LLM infrastructure from core
# LLM embedding and text generation models are available via core terraform state

# Create MongoDB connection via Flink SQL statement
resource "confluent_flink_statement" "mongodb_connection_statement_lab2" {
  organization {
    id = data.terraform_remote_state.core.outputs.confluent_organization_id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.terraform_remote_state.core.outputs.confluent_flink_rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement_name = "mongodb-connection-create"

  statement = <<-EOT
    CREATE CONNECTION IF NOT EXISTS `${data.terraform_remote_state.core.outputs.confluent_environment_display_name}`.`${data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name}`.`mongodb-connection`
    WITH (
      'type' = 'MONGODB',
      'endpoint' = '${local.effective_mongodb_conn}',
      'username' = '${local.effective_mongodb_user}',
      'password' = '${local.effective_mongodb_pass}'
    );
  EOT

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    ignore_changes  = [statement]
    prevent_destroy = false
  }

  depends_on = [
    data.terraform_remote_state.core
  ]
}

# Create queries table - basic Kafka table for query input
resource "confluent_flink_statement" "queries_table" {
  statement_name = "create-table-queries"
  organization {
    id = data.terraform_remote_state.core.outputs.confluent_organization_id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.terraform_remote_state.core.outputs.confluent_flink_rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement = "CREATE TABLE `${data.terraform_remote_state.core.outputs.confluent_environment_display_name}`.`${data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name}`.queries ( query STRING NOT NULL, query_user STRING );"

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = "default"
  }

  lifecycle {
    prevent_destroy = false
  }
}

# Create queries_embed table schema first
resource "confluent_flink_statement" "queries_embed_table" {
  statement_name = "create-table-queries-embed"
  organization {
    id = data.terraform_remote_state.core.outputs.confluent_organization_id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.terraform_remote_state.core.outputs.confluent_flink_rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement = "CREATE TABLE `${data.terraform_remote_state.core.outputs.confluent_environment_display_name}`.`${data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name}`.queries_embed ( query STRING, embedding ARRAY<FLOAT> );"

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [confluent_flink_statement.queries_table]
}

# Sample data insertion - insert one query for testing
resource "confluent_flink_statement" "queries_insert_sample" {
  statement_name = "queries-insert-sample"

  organization {
    id = data.terraform_remote_state.core.outputs.confluent_organization_id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.terraform_remote_state.core.outputs.confluent_flink_rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement = "INSERT INTO queries (query) VALUES ('How do I create a Flink table?');"

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    confluent_flink_statement.queries_table
  ]
}

# Create documents_vectordb table (MongoDB vector store external table)
resource "confluent_flink_statement" "documents_vectordb_create_table" {
  statement_name = "documents-vectordb-create-table"

  organization {
    id = data.terraform_remote_state.core.outputs.confluent_organization_id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.terraform_remote_state.core.outputs.confluent_flink_rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement = "CREATE TABLE IF NOT EXISTS documents_vectordb_lab2 ( document_id STRING, chunk STRING, embedding ARRAY<FLOAT> ) WITH ( 'connector' = 'mongodb', 'mongodb.connection' = 'mongodb-connection', 'mongodb.database' = '${var.MONGODB_DATABASE}', 'mongodb.collection' = '${var.MONGODB_COLLECTION}', 'mongodb.index' = '${var.MONGODB_INDEX_NAME}', 'mongodb.embedding_column' = 'embedding', 'mongodb.numCandidates' = '500' );"

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    confluent_flink_statement.mongodb_connection_statement_lab2
  ]
}

# Populate queries_embed table with embedded queries
resource "confluent_flink_statement" "queries_embed_insert_into" {
  statement_name = "queries-embed-insert-into"

  organization {
    id = data.terraform_remote_state.core.outputs.confluent_organization_id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.terraform_remote_state.core.outputs.confluent_flink_rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement = "INSERT INTO queries_embed SELECT query, embedding FROM queries, LATERAL TABLE(ML_PREDICT('llm_embedding_model', query));"

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    confluent_flink_statement.queries_embed_table,
    confluent_flink_statement.documents_vectordb_create_table
  ]
}

# Create search_results table with vector search results
resource "confluent_flink_statement" "search_results_create_table" {
  statement_name = "search-results-create-table"

  organization {
    id = data.terraform_remote_state.core.outputs.confluent_organization_id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.terraform_remote_state.core.outputs.confluent_flink_rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement = "CREATE TABLE IF NOT EXISTS search_results AS SELECT qe.query, vs.search_results[1].document_id AS document_id_1, vs.search_results[1].chunk AS chunk_1, vs.search_results[1].score AS score_1, vs.search_results[2].document_id AS document_id_2, vs.search_results[2].chunk AS chunk_2, vs.search_results[2].score AS score_2, vs.search_results[3].document_id AS document_id_3, vs.search_results[3].chunk AS chunk_3, vs.search_results[3].score AS score_3 FROM queries_embed AS qe, LATERAL TABLE(VECTOR_SEARCH_AGG( documents_vectordb_lab2, DESCRIPTOR(embedding), qe.embedding, 3 )) AS vs;"

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    confluent_flink_statement.documents_vectordb_create_table,
    confluent_flink_statement.queries_embed_insert_into
  ]
}

# Create search_results_response table with RAG responses
resource "confluent_flink_statement" "search_results_response_create_table" {
  statement_name = "search-results-response-create-table"

  organization {
    id = data.terraform_remote_state.core.outputs.confluent_organization_id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.terraform_remote_state.core.outputs.confluent_flink_rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement = "CREATE TABLE IF NOT EXISTS search_results_response AS SELECT sr.query, sr.document_id_1, sr.chunk_1, sr.score_1, sr.document_id_2, sr.chunk_2, sr.score_2, sr.document_id_3, sr.chunk_3, sr.score_3, pred.response FROM search_results sr, LATERAL TABLE( ml_predict( 'llm_textgen_model', CONCAT( 'Based on the following search results, provide a helpful and comprehensive response to the user query based upon the relevant retrieved documents. Cite the exact parts of the retrieved documents whenever possible.\\n\\nUSER QUERY: ', COALESCE(sr.query, ''), '\\n\\nSEARCH RESULTS:\\n\\nDocument 1 (Similarity Score: ', COALESCE(CAST(sr.score_1 AS STRING), '0'), '):\\nSource: ', COALESCE(sr.document_id_1, 'unknown'), '\\nContent: ', COALESCE(sr.chunk_1, '(no content)'), '\\n\\nDocument 2 (Similarity Score: ', COALESCE(CAST(sr.score_2 AS STRING), '0'), '):\\nSource: ', COALESCE(sr.document_id_2, 'unknown'), '\\nContent: ', COALESCE(sr.chunk_2, '(no content)'), '\\n\\nDocument 3 (Similarity Score: ', COALESCE(CAST(sr.score_3 AS STRING), '0'), '):\\nSource: ', COALESCE(sr.document_id_3, 'unknown'), '\\nContent: ', COALESCE(sr.chunk_3, '(no content)'), '\\n\\nINSTRUCTIONS:\\n- Synthesize information from the most relevant documents above\\n- Provide specific, actionable guidance when possible\\n- Reference document sources in your response\\n- If the search results don''t contain relevant information, say so clearly\\n\\nRESPONSE:' ) ) ) AS pred;"

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    confluent_flink_statement.search_results_create_table
  ]
}

# Source table backed by the `documents` Kafka topic (written by uv run publish-docs)
resource "confluent_flink_statement" "documents_table" {
  statement_name = "documents-create-table"

  organization {
    id = data.terraform_remote_state.core.outputs.confluent_organization_id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.terraform_remote_state.core.outputs.confluent_flink_rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement = "CREATE TABLE IF NOT EXISTS `${data.terraform_remote_state.core.outputs.confluent_environment_display_name}`.`${data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name}`.`documents` ( document_id STRING, document_text STRING, pages STRING, section_reference STRING, title STRING, fraud_categories ARRAY<STRING>, policy_keywords ARRAY<STRING>, char_count INT, document_publisher STRING );"

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    prevent_destroy = false
  }
}

# Kafka-backed table that stores embedded documents (document_id, chunk, embedding).
# Populated by the streaming INSERT below; a Kafka Connect MongoDB Atlas Sink
# connector should be pointed at this topic to push data into the vector store.
resource "confluent_flink_statement" "documents_embed_table" {
  statement_name = "documents-embed-create-table"

  organization {
    id = data.terraform_remote_state.core.outputs.confluent_organization_id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.terraform_remote_state.core.outputs.confluent_flink_rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement = "CREATE TABLE IF NOT EXISTS `${data.terraform_remote_state.core.outputs.confluent_environment_display_name}`.`${data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name}`.`documents_embed` ( document_id STRING, chunk STRING, embedding ARRAY<FLOAT> );"

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    confluent_flink_statement.documents_table
  ]
}

# Streaming pipeline: read documents, generate embeddings, write to documents_embed topic.
# The MongoDB connector table (documents_vectordb_lab2) is a read-only lookup connector
# and cannot be the target of INSERT statements in Confluent Cloud Flink.
resource "confluent_flink_statement" "documents_embed_insert_into" {
  statement_name = "documents-embed-insert-into"

  organization {
    id = data.terraform_remote_state.core.outputs.confluent_organization_id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.terraform_remote_state.core.outputs.confluent_flink_rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement = "INSERT INTO documents_embed SELECT document_id, document_text AS chunk, embedding FROM documents, LATERAL TABLE(ML_PREDICT('llm_embedding_model', document_text));"

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    confluent_flink_statement.documents_embed_table
  ]
}

# Kafka Connect MongoDB Atlas Sink — reads from documents_embed topic and writes
# (document_id, chunk, embedding) into the MongoDB collection used by VECTOR_SEARCH_AGG.
resource "confluent_connector" "documents_embed_sink" {
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  kafka_cluster {
    id = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_id
  }

  config_nonsensitive = {
    "connector.class"          = "MongoDbAtlasSink"
    "name"                     = "documents-embed-sink"
    "kafka.auth.mode"          = "SERVICE_ACCOUNT"
    "kafka.service.account.id" = data.terraform_remote_state.core.outputs.app_manager_service_account_id
    "topics"                   = "documents_embed"
    "connection.host"          = local.mongodb_host
    "database"                 = var.MONGODB_DATABASE
    "collection"               = var.MONGODB_COLLECTION
    "tasks.max"                = "1"
    "input.data.format"        = "AVRO"
    "schema.context.name"      = "default"
  }

  config_sensitive = {
    "connection.user"     = local.effective_mongodb_user
    "connection.password" = local.effective_mongodb_pass
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    confluent_flink_statement.documents_embed_insert_into
  ]
}

# Set topic retention on all Lab2 Kafka topics after they are created by Flink (value defined in scripts/common/confluent_rest.py)
resource "null_resource" "set_lab2_topic_retention" {
  triggers = {
    documents                = confluent_flink_statement.documents_table.id
    documents_embed          = confluent_flink_statement.documents_embed_table.id
    queries                  = confluent_flink_statement.queries_table.id
    queries_embed            = confluent_flink_statement.queries_embed_table.id
    search_results           = confluent_flink_statement.search_results_create_table.id
    search_results_response  = confluent_flink_statement.search_results_response_create_table.id
  }

  provisioner "local-exec" {
    command = "cd '${path.module}/../..' && uv run python scripts/set_topic_retention.py"
    environment = {
      TOPICS        = "documents documents_embed queries queries_embed search_results search_results_response"
      CLUSTER_ID    = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_id
      REST_ENDPOINT = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_rest_endpoint
      KAFKA_KEY     = data.terraform_remote_state.core.outputs.app_manager_kafka_api_key
      KAFKA_SECRET  = data.terraform_remote_state.core.outputs.app_manager_kafka_api_secret
    }
  }

  depends_on = [
    confluent_flink_statement.documents_table,
    confluent_flink_statement.documents_embed_table,
    confluent_flink_statement.queries_table,
    confluent_flink_statement.queries_embed_table,
    confluent_flink_statement.search_results_create_table,
    confluent_flink_statement.search_results_response_create_table,
  ]
}

# Generate Flink SQL command summary
resource "null_resource" "generate_flink_sql_summary" {
  # Trigger regeneration when key resources change
  triggers = {
    queries_table         = confluent_flink_statement.queries_table.id
    mongodb_connection    = confluent_flink_statement.mongodb_connection_statement_lab2.id
  }

  provisioner "local-exec" {
    command     = "cd ${path.module}/../.. && uv run generate_summaries ${local.cloud_provider}"
    working_dir = path.module
  }

  depends_on = [
    confluent_flink_statement.queries_table,
    confluent_flink_statement.queries_embed_table,
    confluent_flink_statement.documents_vectordb_create_table,
    confluent_flink_statement.documents_table,
    confluent_flink_statement.documents_embed_table,
    confluent_flink_statement.documents_embed_insert_into,
    confluent_connector.documents_embed_sink,
    confluent_flink_statement.search_results_create_table,
    confluent_flink_statement.search_results_response_create_table
  ]
}

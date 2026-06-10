# Reference to core infrastructure
data "terraform_remote_state" "core" {
  backend = "local"
  config = {
    path = "../core/terraform.tfstate"
  }
}

data "confluent_organization" "main" {}

data "confluent_flink_region" "quiz_flink_region" {
  cloud  = upper(data.terraform_remote_state.core.outputs.cloud_provider)
  region = data.terraform_remote_state.core.outputs.cloud_region
}

# Kafka topic for collecting participant quiz answers
resource "confluent_flink_statement" "quiz_answers_table" {
  organization {
    id = data.confluent_organization.main.id
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
  rest_endpoint = data.confluent_flink_region.quiz_flink_region.rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement_name = "create-table-quiz-answers"
  statement      = "CREATE TABLE IF NOT EXISTS `${data.terraform_remote_state.core.outputs.confluent_environment_display_name}`.`${data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name}`.`quiz_answers` ( participant STRING, question_number STRING, answer STRING );"

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    ignore_changes = [statement]
  }

  depends_on = [
    data.terraform_remote_state.core
  ]
}

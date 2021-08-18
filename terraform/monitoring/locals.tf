locals {
  lambda_error_alerts_topic_arn = data.terraform_remote_state.monitoring.outputs.storage_lambda_error_alerts_topic_arn
}
module "lambda" {
  source = "../../modules/lambda"

  name        = var.name
  module_name = "end_to_end_bag_test"
  description = var.description

  environment = var.environment

  s3_bucket = "wellcomecollection-storage-infra"
  s3_key    = "lambdas/monitoring/end_to_end_bag_test.zip"

  timeout = 15

  lambda_error_alerts_topic_arn = var.lambda_error_alerts_topic_arn
}

output "function_name" {
  value = module.lambda.function_name
}

output "function_arn" {
  value = module.lambda.arn
}

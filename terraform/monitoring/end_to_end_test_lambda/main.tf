module "lambda" {
  source = "../../modules/lambda"

  name        = var.name
  module_name = "end_to_end_bag_test"
  description = var.description

  environment = var.environment

  s3_bucket = "wellcomecollection-storage-infra"
  s3_key    = "lambdas/monitoring/end_to_end_bag_test.zip"

  timeout = 5
}


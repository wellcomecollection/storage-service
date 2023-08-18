data "aws_s3_object" "end_to_end_bag_test" {
  bucket = "wellcomecollection-storage-infra"
  key    = "lambdas/monitoring/end_to_end_bag_test.zip"
}

moved {
  from = module.lambda2
  to   = module.end_to_end_bag_test
}

module "end_to_end_bag_test" {
  source = "github.com/wellcomecollection/terraform-aws-lambda.git?ref=v1.2.0"

  name        = var.name
  description = var.description

  handler = "end_to_end_bag_test.main"
  runtime = "python3.8"

  environment = {
    variables = var.environment
  }

  s3_bucket         = data.aws_s3_object.end_to_end_bag_test.bucket
  s3_key            = data.aws_s3_object.end_to_end_bag_test.key
  s3_object_version = data.aws_s3_object.end_to_end_bag_test.version_id

  timeout = 15

  error_alarm_topic_arn = var.lambda_error_alerts_topic_arn
}

data "aws_s3_object" "package" {
  bucket = var.s3_bucket
  key    = var.s3_key
}

moved {
  from = aws_lambda_function.lambda_function
  to   = module.lambda.aws_lambda_function.main
}

moved {
  from = aws_iam_role.iam_role
  to   = module.lambda.aws_iam_role.lambda
}

moved {
  from = aws_cloudwatch_metric_alarm.lambda
  to   = module.lambda.aws_cloudwatch_metric_alarm.lambda_errors["arn:aws:sns:eu-west-1:975596993436:storage_lambda_error_alarm"]
}

moved {
  from = aws_cloudwatch_log_group.cloudwatch_log_group
  to   = module.lambda.aws_cloudwatch_log_group.lambda
}

module "lambda" {
  source = "github.com/wellcomecollection/terraform-aws-lambda.git?ref=v1.2.0"

  name        = var.name
  description = var.description

  error_alarm_topic_arn = var.lambda_error_alerts_topic_arn

  s3_bucket         = var.s3_bucket
  s3_key            = var.s3_key
  s3_object_version = data.aws_s3_object.package.version_id

  handler = var.module_name == "" ? "${var.name}.main" : "${var.module_name}.main"
  runtime = "python3.8"
  timeout = var.timeout

  memory_size = var.memory_size

  environment = {
    variables = var.environment
  }
}

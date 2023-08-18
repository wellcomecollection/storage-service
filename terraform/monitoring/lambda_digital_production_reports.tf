data "archive_file" "digital_production_report_lambda" {
  type        = "zip"
  source_file = "${path.module}/../../monitoring/digital_production_report/digital_production_report.py"
  output_path = "${path.module}/digital_production_report.zip"
}

resource "aws_s3_object" "digital_production_report_lambda" {
  bucket = "wellcomecollection-storage-infra"
  key    = "lambdas/monitoring/digital_production_report.zip"
  source = "${path.module}/digital_production_report.zip"
  etag   = data.archive_file.digital_production_report_lambda.output_md5
}

module "digital_production_report_lambda" {
  source = "../modules/lambda"

  name        = "digital_production_report"
  description = "Create monthly/quarterly reports for the Digital Production team"

  s3_bucket = aws_s3_object.digital_production_report_lambda.bucket
  s3_key    = aws_s3_object.digital_production_report_lambda.key

  timeout = 300

  lambda_error_alerts_topic_arn = local.lambda_error_alerts_topic_arn
}

data "aws_iam_policy_document" "read_reporting_secrets" {
  statement {
    actions = [
      "secretsmanager:GetSecretValue",
    ]

    resources = concat(
      module.reporting_secrets.arns,
      [data.aws_secretsmanager_secret_version.storage_service_reporter_slack_webhook.arn],
    )
  }
}

resource "aws_iam_role_policy" "allow_production_report_to_read_secrets" {
  role   = module.digital_production_report_lambda.role_name
  policy = data.aws_iam_policy_document.read_reporting_secrets.json
}

# Schedule the reporter to run at 7am on the first of every month

resource "aws_cloudwatch_event_rule" "first_of_month_at_7am" {
  name                = "trigger_digital_production_report"
  schedule_expression = "cron(0 7 1 * ? *)"
}

resource "aws_lambda_permission" "allow_production_report_cloudwatch_trigger" {
  action        = "lambda:InvokeFunction"
  function_name = module.digital_production_report_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.first_of_month_at_7am.arn
}

resource "aws_cloudwatch_event_target" "first_of_month_at_7am" {
  rule = aws_cloudwatch_event_rule.first_of_month_at_7am.name
  arn  = module.digital_production_report_lambda.arn
}

data "aws_s3_object" "daily_reporter" {
  bucket = "wellcomecollection-storage-infra"
  key    = "lambdas/monitoring/daily_reporter.zip"
}

module "daily_reporter" {
  source = "github.com/wellcomecollection/terraform-aws-lambda.git?ref=v1.2.0"

  name        = "daily_reporter"
  description = "Publish a daily report of storage service activity to Slack"

  handler = "daily_reporter.main"
  runtime = "python3.10"

  s3_bucket         = data.aws_s3_object.daily_reporter.bucket
  s3_key            = data.aws_s3_object.daily_reporter.key
  s3_object_version = data.aws_s3_object.daily_reporter.version_id

  timeout = 300

  error_alarm_topic_arn = local.lambda_error_alerts_topic_arn
}

data "aws_secretsmanager_secret_version" "storage_service_reporter_slack_webhook" {
  secret_id = "storage_service_reporter/slack_webhook"
}

data "aws_secretsmanager_secret_version" "storage_service_reporter_es_host" {
  secret_id = "storage_service_reporter/es_host"
}

data "aws_secretsmanager_secret_version" "storage_service_reporter_es_port" {
  secret_id = "storage_service_reporter/es_port"
}

data "aws_secretsmanager_secret_version" "storage_service_reporter_es_user" {
  secret_id = "storage_service_reporter/es_user"
}

data "aws_secretsmanager_secret_version" "storage_service_reporter_es_pass" {
  secret_id = "storage_service_reporter/es_pass"
}

data "aws_iam_policy_document" "read_es_secrets" {
  statement {
    actions = [
      "secretsmanager:GetSecretValue",
    ]

    resources = [
      data.aws_secretsmanager_secret_version.storage_service_reporter_slack_webhook.arn,
      data.aws_secretsmanager_secret_version.storage_service_reporter_es_host.arn,
      data.aws_secretsmanager_secret_version.storage_service_reporter_es_port.arn,
      data.aws_secretsmanager_secret_version.storage_service_reporter_es_user.arn,
      data.aws_secretsmanager_secret_version.storage_service_reporter_es_pass.arn,
    ]
  }
}

resource "aws_iam_role_policy" "allow_reporter_to_read_es_secrets" {
  role   = module.daily_reporter.lambda_role.name
  policy = data.aws_iam_policy_document.read_es_secrets.json
}

data "aws_iam_policy_document" "upload_to_s3" {
  statement {
    actions = [
      "s3:Put*",
    ]

    resources = [
      "${aws_s3_bucket.daily_reporter.arn}/*",
    ]
  }
}

resource "aws_iam_role_policy" "allow_reporter_to_upload_to_s3" {
  role   = module.daily_reporter.lambda_role.name
  policy = data.aws_iam_policy_document.upload_to_s3.json
}

# Schedule the reporter to run at 6am every day

resource "aws_cloudwatch_event_rule" "every_day_at_6am" {
  name                = "trigger_daily_reporter"
  schedule_expression = "cron(0 6 * * ? *)"
}

resource "aws_lambda_permission" "allow_reporter_cloudwatch_trigger" {
  action        = "lambda:InvokeFunction"
  function_name = module.daily_reporter.lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.every_day_at_6am.arn
}

resource "aws_cloudwatch_event_target" "event_trigger" {
  rule = aws_cloudwatch_event_rule.every_day_at_6am.name
  arn  = module.daily_reporter.lambda.arn
}

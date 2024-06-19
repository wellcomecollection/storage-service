data "aws_s3_object" "ingest_inspector" {
  bucket = "wellcomecollection-storage-infra"
  key    = "lambdas/monitoring/ingest_inspector_backend.zip"
}

module "ingest_inspector_lambda" {
  source = "git@github.com:wellcomecollection/terraform-aws-lambda?ref=v1.2.0"

  name        = "ingest_inspector_backend"
  description = "API endpoint for the Ingest Inspector app."
  runtime     = "python3.10"

  s3_bucket         = data.aws_s3_object.ingest_inspector.bucket
  s3_key            = data.aws_s3_object.ingest_inspector.key
  s3_object_version = data.aws_s3_object.ingest_inspector.version_id

  handler     = "ingest_inspector_backend.lambda_handler"
  memory_size = 512
  timeout     = 60 // 1 minute

  environment = {
    variables = {
      "HOME" = "dummy_value"
    }
  }

  #  error_alarm_topic_arn = local.lambda_error_alerts_topic_arn
}

data "aws_iam_policy_document" "allow_secret_read" {
  statement {
    actions = ["secretsmanager:GetSecretValue"]
    resources = [
      aws_secretsmanager_secret.ingest_inspector_cognito_client_id.arn,
      aws_secretsmanager_secret.ingest_inspector_cognito_client_secret.arn
    ]
  }
}

resource "aws_iam_role_policy" "read_secrets_policy" {
  role   = module.ingest_inspector_lambda.lambda_role.name
  policy = data.aws_iam_policy_document.allow_secret_read.json
}

resource "aws_apigatewayv2_api" "ingest_inspector_api" {
  name          = "Ingest Inspector API"
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_stage" "ingest_inspector_api_v1" {
  api_id = aws_apigatewayv2_api.ingest_inspector_api.id

  name        = "v1"
  auto_deploy = true
}

resource "aws_apigatewayv2_integration" "ingest_inspector_api_lambda_integration" {
  api_id = aws_apigatewayv2_api.ingest_inspector_api.id

  integration_uri    = module.ingest_inspector_lambda.lambda.invoke_arn
  integration_type   = "AWS_PROXY"
  integration_method = "POST"
}

resource "aws_apigatewayv2_route" "get_ingest_endpoint" {
  api_id = aws_apigatewayv2_api.ingest_inspector_api.id

  route_key = "GET /ingest/{ingest_id}"
  target    = "integrations/${aws_apigatewayv2_integration.ingest_inspector_api_lambda_integration.id}"
}

resource "aws_lambda_permission" "allow_api_gateway_to_trigger_lambda" {
  statement_id  = "AllowExecutionFromAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = module.ingest_inspector_lambda.lambda.function_name
  principal     = "apigateway.amazonaws.com"

  source_arn = "${aws_apigatewayv2_api.ingest_inspector_api.execution_arn}/*/*"
}

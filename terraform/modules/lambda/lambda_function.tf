data "aws_s3_bucket_object" "package" {
  bucket = var.s3_bucket
  key    = var.s3_key
}

resource "aws_lambda_function" "lambda_function" {
  description   = var.description
  function_name = var.name

  s3_bucket         = var.s3_bucket
  s3_key            = var.s3_key
  s3_object_version = data.aws_s3_bucket_object.package.version_id

  role    = aws_iam_role.iam_role.arn
  handler = var.module_name == "" ? "${var.name}.main" : "${var.module_name}.main"
  runtime = "python3.6"
  timeout = var.timeout

  memory_size = var.memory_size

  environment {
    variables = var.environment
  }
}

# Note: S3 buckets only support a single notification configuration.
#
# This must be the *only* bucket notification configured for the primary storage
# bucket in the stack, or the extra policies will be replaced whenever this
# stack is applied.

resource "aws_s3_bucket_notification" "primary_storage" {
  bucket = var.replica_primary_bucket_name

  lambda_function {
    lambda_function_arn = module.s3_object_tagger.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_s3_object_tagger]
}

resource "aws_lambda_permission" "allow_s3_object_tagger" {
  action        = "lambda:InvokeFunction"
  function_name = module.s3_object_tagger.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = "arn:aws:s3:::${var.replica_primary_bucket_name}"
}

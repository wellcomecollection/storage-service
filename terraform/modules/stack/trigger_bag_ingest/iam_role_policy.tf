resource "aws_iam_role_policy" "lambda_trigger_bag_ingest_kms" {
  name   = "lambda_trigger_bag_ingest_use_encryption_key"
  role   = "${module.lambda_trigger_bag_ingest_iam.role_name}"
  policy = "${var.use_encryption_key_policy}"
}

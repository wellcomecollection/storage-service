# Give the Lambda permission to read the storage service credentials that
# are kept in Secrets Manager.

data "aws_secretsmanager_secret_version" "client_id" {
  secret_id = "end_to_end_bag_tester/client_id"
}

data "aws_secretsmanager_secret_version" "client_secret" {
  secret_id = "end_to_end_bag_tester/client_secret"
}

data "aws_iam_policy_document" "read_end_to_end_secrets" {
  statement {
    actions = [
      "secretsmanager:GetSecretValue",
    ]

    resources = [
      data.aws_secretsmanager_secret_version.client_id.arn,
      data.aws_secretsmanager_secret_version.client_secret.arn,
    ]
  }
}

resource "aws_iam_role_policy" "allow_read_secrets" {
  role   = module.lambda.role_name
  policy = data.aws_iam_policy_document.read_end_to_end_secrets.json
}

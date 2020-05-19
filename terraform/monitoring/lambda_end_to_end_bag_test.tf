module "end_to_end_bag_tester_stage" {
  source = "../modules/lambda"

  name        = "end_to_end_bag_test--staging"
  module_name = "end_to_end_bag_test"
  description = "Send a bag to test the staging storage service"

  environment = {
    BUCKET = module.bag_with_fetch_file_stage.bucket
    KEY    = module.bag_with_fetch_file_stage.key

    EXTERNAL_IDENTIFIER = "test_bag"

    INGEST_TYPE = "update"

    API_URL = "https://api-stage.wellcomecollection.org/storage/v1"
  }

  s3_bucket = "wellcomecollection-storage-infra"
  s3_key    = "lambdas/monitoring/end_to_end_bag_test.zip"

  timeout = 5
}

module "end_to_end_bag_tester_prod" {
  source = "../modules/lambda"

  name        = "end_to_end_bag_test"
  module_name = "end_to_end_bag_test"
  description = "Send a bag to test the storage service"

  environment = {
    BUCKET = module.bag_with_fetch_file_prod.bucket
    KEY    = module.bag_with_fetch_file_prod.key

    EXTERNAL_IDENTIFIER = "test_bag"

    INGEST_TYPE = "update"

    API_URL = "https://api.wellcomecollection.org/storage/v1"
  }

  s3_bucket = "wellcomecollection-storage-infra"
  s3_key    = "lambdas/monitoring/end_to_end_bag_test.zip"

  timeout = 5
}

# Give the Lambdas permission to read the storage service credentials that
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

resource "aws_iam_role_policy" "allow_end_to_end_staging_read_secrets" {
  role   = module.end_to_end_bag_tester_stage.role_name
  policy = data.aws_iam_policy_document.read_end_to_end_secrets.json
}

resource "aws_iam_role_policy" "allow_end_to_end_prod_read_secrets" {
  role   = module.end_to_end_bag_tester_prod.role_name
  policy = data.aws_iam_policy_document.read_end_to_end_secrets.json
}

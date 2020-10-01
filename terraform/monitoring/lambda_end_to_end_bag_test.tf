module "end_to_end_bag_tester_stage" {
  source = "./end_to_end_test_lambda"

  name        = "end_to_end_bag_test--staging"
  description = "Send a bag to test the staging storage service"

  environment = {
    BUCKET = local.infra_bucket
    KEY    = "test_bags/bag_with_fetch_file_stage.tar.gz"

    EXTERNAL_IDENTIFIER = "test_bag"

    INGEST_TYPE = "update"

    API_URL = "https://api-stage.wellcomecollection.org/storage/v1"
  }

  tags = local.default_tags
}

module "end_to_end_bag_tester_prod" {
  source = "./end_to_end_test_lambda"

  name        = "end_to_end_bag_test"
  description = "Send a bag to test the storage service"

  environment = {
    BUCKET = local.infra_bucket
    KEY    = "test_bags/bag_with_fetch_file_prod.tar.gz"

    EXTERNAL_IDENTIFIER = "test_bag"

    INGEST_TYPE = "update"

    API_URL = "https://api.wellcomecollection.org/storage/v1"
  }

  tags = local.default_tags
}

# Allow the CI agent running in BuildKite to trigger the Lambda after
# deploying new images to staging.
# See https://github.com/wellcomecollection/platform/issues/4819
resource "aws_lambda_permission" "allow_platform_to_trigger_lambda" {
  action        = "lambda:InvokeFunction"
  function_name = module.end_to_end_bag_tester_stage.function_name
  principal     = "arn:aws:iam::760097843905:root"  # platform account
}

data "aws_iam_policy_document" "allow_ci_to_trigger_test_lambda" {
  statement {
    actions = [
      "lambda:InvokeFunction"
    ]

    resources = [
      module.end_to_end_bag_tester_stage.function_arn
    ]
  }
}

resource "aws_iam_role_policy" "allow_ci_to_trigger_test_lambda" {
  role   = data.terraform_remote_state.builds_infra.outputs.ci_role_name
  policy = data.aws_iam_policy_document.allow_ci_to_trigger_test_lambda.json

  provider = aws.platform
}

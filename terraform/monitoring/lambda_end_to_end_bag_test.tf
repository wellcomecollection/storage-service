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

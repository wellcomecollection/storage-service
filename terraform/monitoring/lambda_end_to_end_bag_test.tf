module "end_to_end_bag_tester_stage" {
  source = "./end_to_end_test_lambda"

  name = "end_to_end_bag_test--staging"
  description = "Send a bag to test the staging storage service"

  environment = {
    BUCKET = module.bag_with_fetch_file_stage.bucket
    KEY    = module.bag_with_fetch_file_stage.key

    EXTERNAL_IDENTIFIER = "test_bag"

    INGEST_TYPE = "update"

    API_URL = "https://api-stage.wellcomecollection.org/storage/v1"
  }
}

module "end_to_end_bag_tester_prod" {
  source = "./end_to_end_test_lambda"

  name = "end_to_end_bag_test"
  description = "Send a bag to test the storage service"

  environment = {
    BUCKET = module.bag_with_fetch_file_prod.bucket
    KEY    = module.bag_with_fetch_file_prod.key

    EXTERNAL_IDENTIFIER = "test_bag"

    INGEST_TYPE = "update"

    API_URL = "https://api.wellcomecollection.org/storage/v1"
  }
}

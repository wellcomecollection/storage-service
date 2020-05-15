locals {
  wc_user_pool_id = "eu-west-1_h3qKmdYyD"
}

resource "aws_cognito_user_pool_client" "catalogue" {
  name = "Catalogue"

  user_pool_id = local.wc_user_pool_id

  allowed_oauth_flows = [
    "client_credentials",
  ]

  allowed_oauth_flows_user_pool_client = true

  allowed_oauth_scopes = [
    "https://api.wellcomecollection.org/storage/v1/bags",
    "https://api.wellcomecollection.org/storage/v1/ingests",
  ]

  supported_identity_providers = [
    "COGNITO",
  ]
}

module "alex_home_imac_client" {
  source = "../modules/app_client"

  name         = "Alex home iMac"
  user_pool_id = local.wc_user_pool_id

  allow_bags_access    = false
  allow_ingests_access = true
}

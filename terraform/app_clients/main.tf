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

resource "aws_cognito_user_pool_client" "alex_home" {
  name = "Alex home iMac"

  user_pool_id = local.wc_user_pool_id

  allowed_oauth_flows = [
    "client_credentials",
  ]

  # Error: Error creating Cognito User Pool Client: InvalidOAuthFlowException: client_credentials flow can not be selected if client does not have a client secret.
  generate_secret = true

  allowed_oauth_flows_user_pool_client = true

  allowed_oauth_scopes = [
    "https://api-stage.wellcomecollection.org/storage/v1/bags",
    "https://api-stage.wellcomecollection.org/storage/v1/ingests",
  ]

  supported_identity_providers = [
    "COGNITO",
  ]
}

locals {
  bags_oauth_scopes = var.allow_bags_access == false ? [] : [
    "https://api-stage.wellcomecollection.org/storage/v1/bags",
    "https://api.wellcomecollection.org/storage/v1/bags",
  ]

  ingests_oauth_scopes = var.allow_ingests_access == false ? [] : [
    "https://api-stage.wellcomecollection.org/storage/v1/ingests",
    "https://api.wellcomecollection.org/storage/v1/ingests",
  ]

  oauth_scopes = sort(concat(
    local.bags_oauth_scopes,
    local.ingests_oauth_scopes
  ))
}

resource "aws_cognito_user_pool_client" "client" {
  name         = var.name
  user_pool_id = var.user_pool_id

  allowed_oauth_flows = [
    "client_credentials",
  ]

  # Set this flag so a client secret gets automatically generated for you.
  # If you don't, you get an error at apply time:
  #
  #   Error creating Cognito User Pool Client: InvalidOAuthFlowException:
  #   client_credentials flow can not be selected if client does not have
  #   a client secret.
  #
  # This defaults to true - but if you are importing a client you may need
  # to toggle this off to prevent it being regenerate.
  generate_secret = var.generate_secret

  allowed_oauth_flows_user_pool_client = true

  allowed_oauth_scopes = local.oauth_scopes

  explicit_auth_flows = var.explicit_auth_flows

  supported_identity_providers = [
    "COGNITO",
  ]

  refresh_token_validity = var.refresh_token_validity
}
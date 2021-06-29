resource "aws_cognito_user_pool" "pool" {
  name = local.namespace

  admin_create_user_config {
    allow_admin_create_user_only = true
  }

  password_policy {
    minimum_length                   = 8
    temporary_password_validity_days = 7
  }
}

resource "aws_cognito_user_pool_domain" "domain" {
  domain       = local.namespace
  user_pool_id = aws_cognito_user_pool.pool.id
}

resource "aws_cognito_resource_server" "storage_api" {
  identifier = "https://example.org/${local.namespace}"
  name       = "Storage API V1"

  scope {
    scope_name        = "ingests"
    scope_description = "Read and write ingests"
  }

  scope {
    scope_name        = "bags"
    scope_description = "Read bags"
  }

  user_pool_id = aws_cognito_user_pool.pool.id
}

resource "aws_cognito_user_pool_client" "client" {
  name         = "Cognito client"
  user_pool_id = aws_cognito_user_pool.pool.id

  allowed_oauth_flows = [
    "client_credentials",
  ]

  generate_secret = true

  allowed_oauth_flows_user_pool_client = true

  allowed_oauth_scopes = [
    "https://example.org/${local.namespace}/ingests",
    "https://example.org/${local.namespace}/bags",
  ]

  supported_identity_providers = [
    "COGNITO",
  ]

  refresh_token_validity = 30
}

module "client_secrets" {
  source = "github.com/wellcomecollection/storage-service.git//terraform/modules/secrets?ref=b24ea38"

  key_value_map = {
    "client_id"     = aws_cognito_user_pool_client.client.id
    "client_secret" = aws_cognito_user_pool_client.client.client_secret
  }
}
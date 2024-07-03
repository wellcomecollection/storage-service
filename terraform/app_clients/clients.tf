module "goobi_client" {
  source = "../modules/app_client"

  name         = "Goobi"
  user_pool_id = aws_cognito_user_pool.pool.id

  allow_bags_access    = true
  allow_ingests_access = true

  explicit_auth_flows = [
    "CUSTOM_AUTH_FLOW_ONLY",
  ]

  refresh_token_validity = 1

  # This is an imported client, set to null to prevent
  # regenerating a secret
  generate_secret = null
}

module "dds_client" {
  source = "../modules/app_client"

  name         = "DDS"
  user_pool_id = aws_cognito_user_pool.pool.id

  allow_bags_access    = true
  allow_ingests_access = true

  explicit_auth_flows = [
    "CUSTOM_AUTH_FLOW_ONLY",
  ]

  refresh_token_validity = 1

  # This is an imported client, set to null to prevent
  # regenerating a secret
  generate_secret = null
}

module "ingest_inspector_dashboard" {
  source = "../modules/app_client"

  name         = "Ingest Inspector dashboard"
  user_pool_id = aws_cognito_user_pool.pool.id

  allow_bags_access    = false
  allow_ingests_access = true
}

module "catalogue_client" {
  source = "../modules/app_client"

  name         = "Catalogue"
  user_pool_id = aws_cognito_user_pool.pool.id

  allow_bags_access    = true
  allow_ingests_access = true
}

module "end_to_end_client" {
  source = "../modules/app_client"

  name         = "End-to-end Lambda tester"
  user_pool_id = aws_cognito_user_pool.pool.id

  allow_bags_access    = true
  allow_ingests_access = true
}

module "dev_client" {
  source = "../modules/app_client"

  name         = "Storage service developers"
  user_pool_id = aws_cognito_user_pool.pool.id

  allow_bags_access    = true
  allow_ingests_access = true
}

module "buildkite_client" {
  source = "../modules/app_client"

  name         = "Buildkite ingest testing"
  user_pool_id = aws_cognito_user_pool.pool.id

  allow_bags_access    = false
  allow_ingests_access = true
}

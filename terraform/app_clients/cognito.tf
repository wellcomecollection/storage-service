resource "aws_cognito_user_pool" "pool" {
  name = "Wellcome Collection"

  admin_create_user_config {
    allow_admin_create_user_only = true
  }

  password_policy {
    minimum_length                   = 8
    temporary_password_validity_days = 7
  }
}

resource "aws_cognito_user_pool_domain" "domain" {
  domain          = "auth.wellcomecollection.org"
  certificate_arn = data.aws_acm_certificate.auth.arn
  user_pool_id    = aws_cognito_user_pool.pool.id
}

resource "aws_cognito_resource_server" "storage_api" {
  identifier = "https://api.wellcomecollection.org/storage/v1"
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

data "aws_acm_certificate" "auth" {
  domain   = "auth.wellcomecollection.org"
  statuses = ["ISSUED"]

  provider = aws.us-east-1
}
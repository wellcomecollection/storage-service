resource "aws_secretsmanager_secret" "ingest_inspector_cognito_client_id" {
  name = "ingest-inspector-backend/cognito-client-id"
}

resource "aws_secretsmanager_secret" "ingest_inspector_cognito_client_secret" {
  name = "ingest-inspector-backend/cognito-client-secret"
}


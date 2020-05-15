output "id" {
  value = aws_cognito_user_pool_client.client.id
}

output "secret" {
  value = aws_cognito_user_pool_client.client.client_secret
}

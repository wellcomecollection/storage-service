# This URL opens the section of the Cognito Console where you can see all
# your app clients, and in particular find their client IDs and secrets.
#
# This is preferable to dumping those directly into Terraform output.
output "cognito_user_pool_url" {
  value = "https://eu-west-1.console.aws.amazon.com/cognito/users/?region=eu-west-1#/pool/${aws_cognito_user_pool.pool.id}/clients?_k=czaich"
}

output "cognito_user_pool_arn" {
  value = aws_cognito_user_pool.pool.arn
}

output "cognito_storage_api_identifier" {
  value = aws_cognito_resource_server.storage_api.identifier
}
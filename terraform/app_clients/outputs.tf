# This URL opens the section of the Cognito Console where you can see all
# your app clients, and in particular find their client IDs and secrets.
#
# This is preferable to dumping those directly into Terraform output.
output "cognito_user_pool_url" {
  value = "https://eu-west-1.console.aws.amazon.com/cognito/users/?region=eu-west-1#/pool/${local.wc_user_pool_id}/clients?_k=czaich"
}

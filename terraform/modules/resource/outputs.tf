output "integration_uris" {
  value = [
    aws_api_gateway_integration.auth_subresource.uri,
    aws_api_gateway_integration.auth_resource.uri,
  ]
}

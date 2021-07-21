output "integration_uris" {
  value = [
    aws_api_gateway_integration.auth_subresource.uri,
    aws_api_gateway_integration.auth_resource.uri,
  ]
}

output "api_deployment_resource_fingerprint" {
  description = "An opaque value which changes if the module's API Gateway resources change."
  value = sha1(jsonencode([
    aws_api_gateway_integration.auth_resource,
    aws_api_gateway_integration.auth_subresource,
    aws_api_gateway_method.auth_resource,
    aws_api_gateway_method.auth_subresource,
    aws_api_gateway_resource.auth_resource,
    aws_api_gateway_resource.auth_subresource,
  ]))
}

resource "aws_api_gateway_resource" "auth_resource" {
  rest_api_id = var.api_id
  parent_id   = var.root_resource_id
  path_part   = var.path_part
}

resource "aws_api_gateway_method" "auth_resource" {
  rest_api_id = var.api_id
  resource_id = aws_api_gateway_resource.auth_resource.id
  http_method = "ANY"

  authorization        = "COGNITO_USER_POOLS"
  authorizer_id        = var.cognito_id
  authorization_scopes = var.auth_scopes
}

module "auth_resource_integration" {
  source = "git::https://github.com/wellcometrust/terraform.git//api_gateway/modules/integration/proxy?ref=v16.1.8"

  api_id        = var.api_id
  resource_id   = aws_api_gateway_method.auth_resource.resource_id
  connection_id = var.connection_id

  hostname    = var.hostname
  http_method = aws_api_gateway_method.auth_resource.http_method

  forward_port = var.forward_port
  forward_path = var.forward_path
}

resource "aws_api_gateway_resource" "auth_subresource" {
  rest_api_id = var.api_id
  parent_id   = aws_api_gateway_method.auth_resource.resource_id
  path_part   = var.path_part
}

resource "aws_api_gateway_method" "auth_subresource" {
  rest_api_id = var.api_id
  resource_id = aws_api_gateway_resource.auth_subresource.id
  http_method = "ANY"

  authorization        = "COGNITO_USER_POOLS"
  authorizer_id        = var.cognito_id
  authorization_scopes = var.auth_scopes

  request_parameters = {
    "method.request.path.proxy" = true
  }
}

module "auth_subresource_integration" {
  source = "git::https://github.com/wellcometrust/terraform.git//api_gateway/modules/integration/proxy?ref=v16.1.8"

  api_id        = var.api_id
  resource_id   = aws_api_gateway_method.auth_subresource.resource_id
  connection_id = var.connection_id

  hostname    = var.hostname
  http_method = aws_api_gateway_method.auth_subresource.http_method

  forward_port = var.forward_port
  forward_path = "${var.forward_path}/{proxy}"

  request_parameters = {
    "integration.request.path.proxy" = "method.request.path.proxy"
  }
}

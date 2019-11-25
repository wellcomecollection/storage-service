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

resource "aws_api_gateway_integration" "auth_resource" {
  rest_api_id = var.api_id
  resource_id = aws_api_gateway_resource.auth_resource.id
  http_method = aws_api_gateway_method.auth_resource.http_method

  integration_http_method = "ANY"
  type                    = "HTTP_PROXY"
  connection_type         = "VPC_LINK"
  connection_id           = var.connection_id
  uri                     = "http://${var.hostname}:${var.forward_port}/${var.forward_path}"
}

resource "aws_api_gateway_resource" "auth_subresource" {
  rest_api_id = var.api_id
  parent_id   = aws_api_gateway_resource.auth_resource.id
  path_part   = "{proxy+}"
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

resource "aws_api_gateway_integration" "auth_subresource" {
  rest_api_id = var.api_id
  resource_id = aws_api_gateway_resource.auth_subresource.id
  http_method = aws_api_gateway_method.auth_subresource.http_method

  integration_http_method = "ANY"
  type                    = "HTTP_PROXY"
  connection_type         = "VPC_LINK"
  connection_id           = var.connection_id
  uri                     = "http://${var.hostname}:${var.forward_port}/${var.forward_path}/{proxy}"

  request_parameters = {
    "integration.request.path.proxy" = "method.request.path.proxy"
  }
}

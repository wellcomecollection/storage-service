# API

resource "aws_api_gateway_rest_api" "api" {
  name = "Storage API (${var.namespace})"

  endpoint_configuration {
    types = ["REGIONAL"]
  }
}

resource "aws_api_gateway_resource" "resource" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  parent_id   = aws_api_gateway_rest_api.api.root_resource_id
  path_part   = "context.json"
}

resource "aws_api_gateway_method" "root_resource_method" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  resource_id = aws_api_gateway_resource.resource.id
  http_method = "GET"

  authorization = "NONE"
}

# context.json

resource "aws_api_gateway_integration" "root_static_response" {
  rest_api_id             = aws_api_gateway_rest_api.api.id
  resource_id             = aws_api_gateway_resource.resource.id
  http_method             = "GET"
  integration_http_method = "GET"
  type                    = "AWS"
  uri                     = "arn:aws:apigateway:${var.aws_region}:s3:path//${aws_s3_bucket_object.context.bucket}/${aws_s3_bucket_object.context.key}"

  credentials = aws_iam_role.static_resource_role.arn
}

resource "aws_api_gateway_method_response" "root_resource_http_200" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  resource_id = aws_api_gateway_resource.resource.id
  http_method = "GET"
  status_code = "200"
}

resource "aws_api_gateway_integration_response" "root_resource_http_200" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  resource_id = aws_api_gateway_resource.resource.id
  http_method = "GET"
  status_code = aws_api_gateway_method_response.root_resource_http_200.status_code
}

resource "aws_api_gateway_authorizer" "cognito" {
  name          = "cognito"
  type          = "COGNITO_USER_POOLS"
  rest_api_id   = aws_api_gateway_rest_api.api.id
  provider_arns = [var.cognito_user_pool_arn]
}

# Domains

module "domain" {
  source = "./domain"

  domain_name      = var.domain_name
  cert_domain_name = var.cert_domain_name
}

# Stages

locals {
  # We pass the list of integration URIs as a variable to the API Gateway deployment,
  # but it only supports strings with alphanumeric characters and ' ' and ,-._:/?=,
  integration_uri_list = concat(
    module.bags.integration_uris,
    module.ingests.integration_uris,
  )

  integration_uri_str = join("; ", local.integration_uri_list)

  integration_uri_variable = replace(
    local.integration_uri_str,
    "/[^A-Za-z0-9 ,-._:/?=]+/",
    "_"
  )
}

resource "aws_api_gateway_deployment" "v1" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  stage_name  = "v1"

  variables = {
    bags_port          = local.bags_listener_port
    ingests_port       = local.ingests_listener_port,
    static_response_id = aws_api_gateway_integration.root_static_response.id,
    integration_uris   = local.integration_uri_variable
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_api_gateway_stage" "v1" {
  stage_name    = "v1"
  rest_api_id   = aws_api_gateway_rest_api.api.id
  deployment_id = aws_api_gateway_deployment.v1.id

  variables = {
    bags_port          = local.bags_listener_port
    ingests_port       = local.ingests_listener_port,
    static_response_id = aws_api_gateway_integration.root_static_response.id,
    integration_uris   = local.integration_uri_variable
  }
}

# Resources

module "bags" {
  source = "../../resource"

  api_id    = aws_api_gateway_rest_api.api.id
  path_part = "bags"

  root_resource_id = aws_api_gateway_rest_api.api.root_resource_id
  connection_id    = aws_api_gateway_vpc_link.link.id

  cognito_id  = aws_api_gateway_authorizer.cognito.id
  auth_scopes = var.auth_scopes

  forward_port = "$${stageVariables.bags_port}"
  forward_path = "bags"
}

module "ingests" {
  source = "../../resource"

  api_id    = aws_api_gateway_rest_api.api.id
  path_part = "ingests"

  root_resource_id = aws_api_gateway_rest_api.api.root_resource_id
  connection_id    = aws_api_gateway_vpc_link.link.id

  cognito_id  = aws_api_gateway_authorizer.cognito.id
  auth_scopes = var.auth_scopes

  forward_port = "$${stageVariables.ingests_port}"
  forward_path = "ingests"
}

# Link

resource "aws_api_gateway_vpc_link" "link" {
  name        = "${var.namespace}-api_vpc_link"
  target_arns = [aws_lb.nlb.arn]
}


# API

resource "aws_api_gateway_rest_api" "api" {
  name = "Storage API (${var.namespace})"

  endpoint_configuration {
    types = ["REGIONAL"]
  }
}

resource "aws_api_gateway_authorizer" "cognito" {
  name          = "cognito"
  type          = "COGNITO_USER_POOLS"
  rest_api_id   = aws_api_gateway_rest_api.api.id
  provider_arns = [var.cognito_user_pool_arn]
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
    bags_port        = local.bags_listener_port
    ingests_port     = local.ingests_listener_port,
    integration_uris = local.integration_uri_variable
  }

  triggers = {
    # Re-deploy this deployment if any of the resources it contains are updated.
    # This also orders the resources to ensure the deployment is created after
    # all of its consitutent resources are created/updated. See the terraform
    # docs for aws_api_gateway_deployment which suggests this pattern.
    redeployment = sha1(jsonencode([
      aws_api_gateway_authorizer.cognito,
      local.gateway_responses_resource_fingerprint,
      module.bags.api_deployment_resource_fingerprint,
      module.ingests.api_deployment_resource_fingerprint,
    ]))
  }

  lifecycle {
    create_before_destroy = true
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


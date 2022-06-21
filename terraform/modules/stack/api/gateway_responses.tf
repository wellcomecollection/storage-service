module "gateway_responses" {
  source = "github.com/wellcomecollection/terraform-aws-api-gateway-responses.git?ref=v1.1.3"

  rest_api_id = aws_api_gateway_rest_api.api.id
}

locals {
  gateway_responses_resource_fingerprint = module.gateway_responses.fingerprint
}

data "aws_acm_certificate" "certificate" {
  domain   = var.cert_domain_name
  statuses = ["ISSUED"]
}

resource "aws_api_gateway_domain_name" "stage" {
  domain_name = var.domain_name

  regional_certificate_arn = data.aws_acm_certificate.certificate.arn

  endpoint_configuration {
    types = ["REGIONAL"]
  }
}


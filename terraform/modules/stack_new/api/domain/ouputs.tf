output "domain_name" {
  value = aws_api_gateway_domain_name.stage.domain_name
}

output "regional_domain_name" {
  value = aws_api_gateway_domain_name.stage.regional_domain_name
}


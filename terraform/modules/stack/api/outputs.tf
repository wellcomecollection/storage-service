output "api_gateway_id" {
  value = aws_api_gateway_rest_api.api.id
}

output "loadbalancer_arn" {
  value = aws_lb.nlb.arn
}

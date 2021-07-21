output "api_gateway_id" {
  value = aws_api_gateway_rest_api.api.id
}

output "loadbalancer_arn" {
  value = aws_lb.nlb.arn
}

output "invoke_url" {
  value = aws_api_gateway_stage.v1.invoke_url
}

output "elasticsearch_host" {
  value = aws_elasticsearch_domain.elasticsearch.endpoint
}

output "token_url" {
  value = local.token_url
}

output "api_url" {
  value = module.stack.api_invoke_url
}

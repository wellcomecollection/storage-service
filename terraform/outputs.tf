output "elasticsearch_host" {
  value = aws_elasticsearch_domain.elasticsearch.endpoint
}

output "api_url" {
  value = aws_elasticsearch_domain.elasticsearch.endpoint
}

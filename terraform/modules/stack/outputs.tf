output "api_gateway_id" {
  value = module.api.api_gateway_id
}

output "interservice_sg_id" {
  value = aws_security_group.interservice.id
}

output "unpacker_task_role_name" {
  value = module.bag_unpacker.task_role_name
}

output "unpacker_task_role_arn" {
  value = module.bag_unpacker.task_role_arn
}

output "api_domain_name" {
  value = module.api.gateway_domain_name
}

output "bag_register_output_topic_arn" {
  value = module.bag_register_output_topic.arn
}


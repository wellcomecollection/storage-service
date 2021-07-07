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

output "registered_bag_notifications_topic_arn" {
  value = module.registered_bag_notifications_topic.arn
}

output "unpacked_bags_bucket_name" {
  value = module.working_storage.unpacked_bags_bucket_name
}

output "api_invoke_url" {
  value = module.api.invoke_url
}

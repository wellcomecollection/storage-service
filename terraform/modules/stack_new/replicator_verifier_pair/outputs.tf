output "verifier_output_topic_name" {
  value = module.bag_verifier_output_topic.name
}

output "verifier_output_topic_arn" {
  value = module.bag_verifier_output_topic.arn
}

output "replicator_task_role_name" {
  value = module.bag_replicator.task_role_name
}

output "verifier_task_role_name" {
  value = module.bag_verifier.task_role_name
}


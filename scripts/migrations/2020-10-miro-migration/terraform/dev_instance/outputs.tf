output "security_group_id" {
  value = aws_security_group.allow_ssh_dev_ip.id
}

output "dev_instance_profile_name" {
  value = aws_iam_instance_profile.dev_instance_profile.name
}
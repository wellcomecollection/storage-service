module "dev_instance" {
  source = "./dev_instance"

  assumable_roles = local.assumable_roles
  dev_name        = ""
}

locals {
  assumable_roles = [
    "arn:aws:iam::975596993436:role/storage-read_only",
    "arn:aws:iam::975596993436:role/storage-developer",
    "arn:aws:iam::299497370133:role/workflow-developer",
    "arn:aws:iam::760097843905:role/platform-read_only"
  ]
}

variable "dev_name" {
  default = "Robert Kenny"
}

output "security_group_id" {
  value = module.dev_instance.security_group_id
}

output "dev_instance_profile_name" {
  value = module.dev_instance.dev_instance_profile_name
}
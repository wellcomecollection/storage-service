variable "subnets" {
  type = list(string)
}

variable "aws_region" {
  default = "eu-west-1"
}

variable "vpc_id" {
}

variable "cluster_arn" {
}

variable "namespace" {
}

variable "namespace_id" {
}

variable "bags_container_image" {
}

variable "bags_container_port" {
}

variable "bags_env_vars" {
  type = map(string)
}

variable "bags_env_vars_length" {
}

variable "bags_nginx_container_image" {
}

variable "bags_nginx_container_port" {
}

variable "ingests_container_image" {
}

variable "ingests_container_port" {
}

variable "ingests_env_vars" {
  type = map(string)
}

variable "ingests_env_vars_length" {
}

variable "ingests_nginx_container_port" {
}

variable "ingests_nginx_container_image" {
}

variable "cognito_user_pool_arn" {
}

variable "auth_scopes" {
  type = list(string)
}

variable "alarm_topic_arn" {
}

variable "static_content_bucket_name" {
}

variable "interservice_security_group_id" {
}

variable "domain_name" {
}

variable "cert_domain_name" {
}

variable "bag_unpacker_topic_arn" {
}

variable "desired_bags_api_count" {
}

variable "desired_ingests_api_count" {
}

variable "use_fargate_spot_for_api" {
  type    = bool
  default = false
}

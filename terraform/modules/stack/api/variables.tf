variable "subnets" {
  type = list(string)
}

variable "aws_region" {
  default = "eu-west-1"
}

variable "vpc_id" {
  type = string
}

variable "cluster_arn" {
  type = string
}

variable "namespace" {
  type = string
}

variable "service_discovery_namespace_id" {
  type = string
}

variable "bags_container_image" {
  type = string
}

variable "bags_container_port" {
  type    = number
  default = 9001
}

variable "bags_environment" {
  type = map(string)
}

variable "ingests_container_image" {
  type = string
}

variable "ingests_container_port" {
  type    = number
  default = 9001
}

variable "ingests_environment" {
  type = map(string)
}

variable "cognito_user_pool_arn" {
  type = string
}

variable "auth_scopes" {
  type = list(string)
}

variable "alarm_topic_arn" {
  type = string
}

variable "static_content_bucket_name" {
  type = string
}

variable "interservice_security_group_id" {
  type = string
}

variable "domain_name" {
  type = string
}

variable "cert_domain_name" {
  type = string
}

variable "bag_unpacker_topic_arn" {
  type = string
}

variable "desired_bags_api_count" {
  type    = number
  default = 3
}

variable "desired_ingests_api_count" {
  type    = number
  default = 3
}

variable "use_fargate_spot_for_api" {
  type    = bool
  default = false
}

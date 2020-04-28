variable "bags_container_image" {
  type = string
}

variable "bags_environment" {
  type = map(string)
}

variable "bags_listener_port" {
  type = number
}

variable "ingests_container_image" {
  type = string
}

variable "ingests_environment" {
  type = map(string)
}

variable "ingests_listener_port" {
  type = number
}

variable "allow_ingests_publish_to_unpacker_topic_json" {
  type = string
}

variable "namespace" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "interservice_security_group_id" {
  type = string
}

variable "load_balancer_arn" {
  type = string
}

variable "service_discovery_namespace_id" {
  type = string
}

variable "subnets" {
  type = list(string)
}

variable "cluster_arn" {
  type = string
}

variable "use_fargate_spot_for_api" {
  type    = bool
  default = false
}
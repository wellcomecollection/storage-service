variable "api_container_image" {
  type = string
}

variable "tracker_container_image" {
  type = string
}

variable "service_name" {
  type = string
}

variable "api_environment" {
  type = map(string)
}

variable "tracker_environment" {
  type = map(string)
}

variable "cpu" {
  type = number
}

variable "memory" {
  type = number
}

variable "container_port" {
  type    = number
  default = 9001
}

variable "service_discovery_namespace_id" {
  type = string
}

variable "use_fargate_spot" {
  type    = bool
  default = false
}

variable "cluster_arn" {
  type = string
}

variable "subnets" {
  type = list(string)
}

variable "security_group_ids" {
  type = list(string)
}

variable "desired_task_count" {
  type    = number
  default = 3
}

variable "vpc_id" {
  type = string
}

variable "load_balancer_arn" {
  type = string
}

variable "load_balancer_listener_port" {
  type = number
}

variable "deployment_service_name" {
  type        = string
  description = "Used by weco-deploy to determine which services to deploy, if unset the value used will be var.name"
  default     = ""
}

variable "deployment_service_env" {
  type        = string
  description = "Used by weco-deploy to determine which services to deploy in conjunction with deployment_service_name"
}

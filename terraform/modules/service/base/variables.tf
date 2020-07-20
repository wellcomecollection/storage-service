variable "service_name" {
  type = string
}

variable "container_definitions" {}

variable "cpu" {
  type = number
}

variable "memory" {
  type = number
}

variable "cluster_arn" {
  type = string
}

variable "service_discovery_namespace_id" {
  type = string
}

variable "subnets" {
  type = list(string)
}

variable "security_group_ids" {
  type = list(string)
}

variable "desired_task_count" {
  type = number
}

variable "use_fargate_spot" {
  type = bool
}

variable "target_group_arn" {
  type    = string
  default = ""
}

variable "container_name" {
  type    = string
  default = ""
}

variable "container_port" {
  type    = string
  default = ""
}

variable "deployment_service_name" {
  type        = string
  description = "Used by weco-deploy to determine which services to deploy, if unset the value used will be var.name"
}
variable "deployment_service_env" {
  type        = string
  description = "Used by weco-deploy to determine which services to deploy in conjunction with deployment_service_name"
}

# Worker

variable "worker_container_image" {
  type = string
}

variable "worker_environment" {
  type    = map(string)
  default = {}
}

variable "worker_secrets" {
  type    = map(string)
  default = {}
}

# Internal

variable "internal_api_environment" {
  type    = map(string)
  default = {}
}

variable "internal_api_secrets" {
  type    = map(string)
  default = {}
}

variable "internal_api_container_image" {
  type = string
}

# External

variable "external_api_container_port" {
  type    = number
  default = 9001
}

variable "external_api_container_image" {
  type = string
}

variable "external_api_environment" {
  type    = map(string)
  default = {}
}

variable "external_api_secrets" {
  type    = map(string)
  default = {}
}

# Shared

variable "service_name" {
  type = string
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

variable "service_discovery_namespace_id" {
  type = string
}

# Target group specific

variable "vpc_id" {
  type = string
}

variable "load_balancer_arn" {
  type = string
}

variable "load_balancer_listener_port" {
  type = number
}

variable "logging_container" {
  description = "Specifies container used for logging within applications."

  type = object({
    container_registry = string
    container_name     = string
    container_tag      = string
  })
}

variable "nginx_container" {
  description = "Specifies container used for the nginx sidecar container."

  type = object({
    container_registry = string
    container_name     = string
    container_tag      = string
  })
}

variable "healthcheck_path" {
  type    = string
  default = "/management/healthcheck"
}
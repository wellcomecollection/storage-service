variable "service_name" {
  type = string
}

variable "container_definitions" {
  type = list(any)
}

variable "cpu" {
  type    = number
  default = 512
}

variable "memory" {
  type    = number
  default = 1024
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
  type    = bool
  default = false
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
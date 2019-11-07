variable "env_vars" {
  type = map(string)
}

variable "env_vars_length" {
}

variable "secret_env_vars" {
  type    = map(string)
  default = {}
}

variable "secret_env_vars_length" {
}

variable "subnets" {
  type = list(string)
}

variable "container_image" {
}

variable "namespace_id" {
}

variable "cluster_name" {
}

variable "cluster_id" {
}

variable "service_name" {
}

variable "min_capacity" {
  default = "1"
}

variable "max_capacity" {
  default = "1"
}

variable "desired_task_count" {
  default = "1"
}

variable "launch_type" {
  default = "FARGATE"
}

variable "security_group_ids" {
  type    = list(string)
  default = []
}

variable "cpu" {
  default = 512
}

variable "memory" {
  default = 1024
}


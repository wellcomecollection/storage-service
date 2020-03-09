variable "env_vars" {
  type = map(string)
}

variable "secret_env_vars" {
  type    = map(string)
  default = {}
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

variable "cluster_arn" {
}

variable "service_name" {
}

variable "desired_task_count" {
  default = 1
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

variable "use_fargate_spot" {
  type    = bool
  default = false
}

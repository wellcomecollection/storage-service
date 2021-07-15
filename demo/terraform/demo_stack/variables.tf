variable "namespace" {
  type = string
}

variable "short_namespace" {
  type = string
}

variable "cidr_block" {
  type    = string
  default = "172.14.0.0/16"
}

# Set the min/max scaling of the ECS tasks.  By default they scale down
# to idle when not in use; increasing the max capacity temporarily will
# warm up the pipeline and get bags stored quickly.
variable "min_capacity" {
  type    = number
  default = 0
}

variable "max_capacity" {
  type    = number
  default = 1
}

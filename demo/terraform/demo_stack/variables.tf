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

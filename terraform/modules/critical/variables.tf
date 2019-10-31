variable "namespace" {}

variable "billing_mode" {
  default     = "PAY_PER_REQUEST"
  description = "Should be either PAY_PER_REQUEST or PROVISIONED"
}

variable "replica_primary_read_principals" {
  type    = "list"
  default = []
}

variable "replica_glacier_read_principals" {
  type    = "list"
  default = []
}

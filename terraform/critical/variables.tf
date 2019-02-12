variable "namespace" {}
variable "account_id" {}
variable "private_cidr_block_ids" {}
variable "vpc_id" {}

variable "service-pl-winslow" {}
variable "service-wt-winnipeg" {}

variable "subnets_ids" {
  type = "list"
}

variable "billing_mode" {
  default     = "PAY_PER_REQUEST"
  description = "Should be either PAY_PER_REQUEST or PROVISIONED"
}

variable "environment_name" {}

variable "archive_readaccess_principles" {
  type    = "list"
  default = []
}

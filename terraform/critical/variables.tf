variable "namespace" {}
variable "account_id" {}
variable "private_cidr_block_ids" {}
variable "vpc_id" {}

variable "service-pl-winslow" {}
variable "service-wt-winnipeg" {}

variable "subnets_ids" {
  type = "list"
}

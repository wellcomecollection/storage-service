variable "namespace" {}
variable "account_id" {}

variable "service-pl-winslow" {}
variable "service-wt-winnipeg" {}

variable "billing_mode" {
  default     = "PAY_PER_REQUEST"
  description = "Should be either PAY_PER_REQUEST or PROVISIONED"
}

variable "archive_read_principles" {
  type    = "list"
  default = []
}

variable "access_read_principles" {
  type    = "list"
  default = []
}

variable "namespace" {}
variable "account_id" {}

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

variable "ingest_read_principles" {
  type    = "list"
  default = []
}

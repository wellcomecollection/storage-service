variable "namespace" {}

variable "billing_mode" {
  default     = "PAY_PER_REQUEST"
  description = "Should be either PAY_PER_REQUEST or PROVISIONED"
}

variable "replicator_primary_task_role_arn" {}
variable "replicator_glacier_task_role_arn" {}

variable "replica_primary_read_principals" {
  type    = list(string)
  default = []
}

variable "enable_s3_versioning" {
  type = bool
}

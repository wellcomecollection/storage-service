variable "namespace" {}

variable "replica_primary_read_principals" {
  type    = list(string)
  default = []
}

variable "enable_s3_versioning" {
  type = bool
}

variable "inventory_bucket" {
  type = string
}

variable "table_name" {
  type    = string
  default = ""
}

variable "azure_resource_group_name" {
  type = string
}

variable "azure_storage_account_name" {
  type = string
}

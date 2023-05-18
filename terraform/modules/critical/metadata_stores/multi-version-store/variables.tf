variable "name" {
  description = "The name of the VHS instance"
  type        = string
}

variable "bucket_name_prefix" {
  description = "A prefix to the S3 bucket name that stores the values"
  default     = ""
  type        = string
}

variable "table_name_prefix" {
  description = "A prefix to the DynamoDB table that stores the keys"
  default     = ""
  type        = string
}

variable "table_name" {
  description = "Overrides default naming scheme to use specified table name"
  type        = string
  default     = ""
}

variable "bucket_name" {
  description = "Overrides default naming scheme to use specified bucket name"
  type        = string
  default     = ""
}

variable "tags" {
  type    = map(string)
  default = {}
}

variable "read_principals" {
  default = []
  type    = list(string)
}

variable "cycle_objects_to_standard_ia" {
  description = "Whether to cycle S3 objects to Standard-IA after 30 days (reduces costs if lookups are infrequent)"

  type    = bool
  default = true
}

variable "namespace" {
  type = string
}

variable "replica_id" {
  type = string
}

variable "replica_display_name" {
  type = string
}

variable "storage_provider" {
  type = string
}

variable "replica_type" {
  type = string
}

variable "topic_arns" {
  type = list(string)
}

variable "destination_namespace" {
  type = string
}

variable "primary_bucket_name" {
  type = string
}

variable "unpacker_bucket_name" {
  type = string
}
variable "bag_verifier_mode" {
  type = string
}

# IAM policies

variable "ingests_read_policy_json" {
  type = string
}

variable "replicator_lock_table_policy_json" {
  type = string
}

# Apps

variable "security_group_ids" {
  type = list(string)
}

variable "cluster_name" {
  type = string
}

variable "cluster_arn" {
  type = string
}

variable "service_discovery_namespace_id" {
  type = string
}

variable "subnets" {
  type = list(string)
}

variable "ingests_topic_arn" {
  type = string
}

variable "replicator_lock_table_name" {
  type = string
}

variable "replicator_lock_table_index" {
  type = string
}

variable "bag_replicator_image" {
  type = string
}

variable "bag_verifier_image" {
  type = string
}

variable "verifier_environment" {
  type    = map(string)
  default = {}
}

variable "verifier_secrets" {
  type    = map(string)
  default = {}
}

variable "replicator_secrets" {
  type    = map(string)
  default = {}
}

variable "deployment_service_name_verifier" {}
variable "deployment_service_name_replicator" {}
variable "deployment_service_env" {}

# Messaging

variable "dlq_alarm_arn" {
  type = string
}

variable "min_capacity" {
  type = number
}

variable "max_capacity" {
  type = number
}

variable "logging_container" {
  description = "Specifies container used for logging within applications."

  type = object({
    container_registry = string
    container_name     = string
    container_tag      = string
  })
}

# Because these operations take a long time (potentially copying thousands
# of S3 objects for a single message), we keep a high visibility timeout to
# avoid messages appearing to time out and fail.
#
# Also, we current lock over the entire destination prefix, and there are
# issues if the lock expires before the message is retried.
#
# See https://github.com/wellcomecollection/storage-service/issues/993
#
variable "replicator_visibility_timeout_seconds" {
  description = "Visibility timeout of the SQS queue for the replicator"
  type        = number
  default     = 60 * 60 * 5
}

variable "verifier_visibility_timeout_seconds" {
  description = "Visibility timeout of the SQS queue for the verifier"
  type        = number
  default     = 60 * 60 * 5
}

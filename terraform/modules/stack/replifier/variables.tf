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

variable "aws_region" {
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

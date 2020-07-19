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

variable "bucket_name" {
  type = string
}

variable "primary_bucket_name" {
  type = string
}

# IAM policies

variable "ingests_read_policy_json" {
  type = string
}

variable "cloudwatch_metrics_policy_json" {
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

variable "secrets" {
  type    = map(string)
  default = {}
}

variable "release_label" {
  type = string
}

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


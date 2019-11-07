variable "namespace" {
}

variable "replica_id" {
}

variable "replica_display_name" {
}

variable "storage_provider" {
}

variable "replica_type" {
}

variable "aws_region" {
}

variable "topic_names" {
  type = list(string)
}

variable "bucket_name" {
}

variable "primary_bucket_name" {
}

# IAM policies

variable "ingests_read_policy_json" {
}

variable "cloudwatch_metrics_policy_json" {
}

variable "replicator_lock_table_policy_json" {
}

# Apps

variable "security_group_ids" {
  type = list(string)
}

variable "cluster_name" {
}

variable "cluster_id" {
}

variable "namespace_id" {
}

variable "subnets" {
  type = list(string)
}

variable "ingests_topic_arn" {
}

variable "logstash_host" {
}

variable "replicator_lock_table_name" {
}

variable "replicator_lock_table_index" {
}

variable "bag_replicator_image" {
}

variable "bag_verifier_image" {
}

# Messaging

variable "dlq_alarm_arn" {
}

variable "min_capacity" {
}

variable "max_capacity" {
}


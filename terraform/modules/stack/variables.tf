variable "namespace" {
  type = string
}

variable "api_url" {
  type = string
}

variable "domain_name" {
  type = string
}

variable "cert_domain_name" {
  type = string
}

variable "dlq_alarm_arn" {
  type = string
}

variable "release_label" {
  type = string
}

# Network

variable "private_subnets" {
  type = list(string)
}

variable "vpc_id" {
  type = string
}

variable "aws_region" {
  default = "eu-west-1"
}


# Storage manifests VHS

variable "vhs_manifests_bucket_name" {
  type = string
}

variable "vhs_manifests_table_name" {
  type = string
}

variable "vhs_manifests_readonly_policy" {
  type = string
}

variable "vhs_manifests_readwrite_policy" {
  type = string
}

# Configuration

variable "replica_primary_bucket_name" {
  type = string
}

variable "replica_glacier_bucket_name" {
  type = string
}

variable "static_content_bucket_name" {
  type = string
}

variable "cognito_storage_api_identifier" {
  type = string
}

variable "cognito_user_pool_arn" {
  type = string
}

variable "alarm_topic_arn" {
  type = string
}

variable "replicas_table_arn" {
  type = string
}

variable "replicas_table_name" {
  type = string
}

variable "ingests_table_name" {
  type = string
}

variable "ingests_table_arn" {
  type = string
}

variable "workflow_bucket_name" {
  type = string
}

# versioner table

variable "versioner_versions_table_arn" {
  type = string
}

variable "versioner_versions_table_name" {
  type = string
}

variable "versioner_versions_table_index" {
  type = string
}

variable "desired_bags_api_count" {
  default = 3
  type = number
}

variable "desired_ingests_api_count" {
  default = 3
  type = number
}

variable "archivematica_ingests_bucket" {
  type = string
}

variable "es_ingests_index_prefix" {
  type = string
}

variable "ingests_indexer_secrets" {
  type = map(string)
}

variable "min_capacity" {
  type = number
}
variable "max_capacity" {
  type = number
}

variable "bag_register_output_subscribe_principals" {
  type = list(string)
}

variable "use_fargate_spot_for_api" {
  type    = bool
  default = false
}

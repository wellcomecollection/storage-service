variable "namespace" {}

variable "api_url" {}
variable "domain_name" {}
variable "cert_domain_name" {}

variable "dlq_alarm_arn" {}

variable "ssh_key_name" {}

variable "release_label" {}

# Network

variable "private_subnets" {
  type = "list"
}

variable "vpc_id" {}

variable "vpc_cidr" {
  type = "list"
}

variable "aws_region" {
  default = "eu-west-1"
}

# Container images

variable "nginx_image" {}

# Storage manifests VHS

variable "vhs_manifests_bucket_name" {}
variable "vhs_manifests_table_name" {}

variable "vhs_manifests_readonly_policy" {}
variable "vhs_manifests_readwrite_policy" {}

# Configuration

variable "replica_primary_bucket_name" {}
variable "replica_glacier_bucket_name" {}

variable "static_content_bucket_name" {}

variable "cognito_storage_api_identifier" {}
variable "cognito_user_pool_arn" {}

variable "alarm_topic_arn" {}

variable "replicas_table_arn" {}
variable "replicas_table_name" {}

variable "ingests_table_name" {}
variable "ingests_table_arn" {}

variable "workflow_bucket_name" {}

# versioner table

variable "versioner_versions_table_arn" {}
variable "versioner_versions_table_name" {}
variable "versioner_versions_table_index" {}

# Task counts

variable "desired_ec2_instances" {
  default = 2
}

# The number of api tasks MUST be one per AZ.  This is due to the behaviour of
# NLBs that seem to increase latency significantly if number of tasks < number of AZs.
variable "desired_bags_api_count" {
  default = 3
}

variable "desired_ingests_api_count" {
  default = 3
}

variable "archivematica_ingests_bucket" {}

variable "min_capacity" {}
variable "max_capacity" {}

variable "namespace" {}

variable "api_url" {}
variable "domain_name" {}
variable "cert_domain_name" {}

variable "lambda_error_alarm_arn" {}
variable "dlq_alarm_arn" {}

variable "infra_bucket" {}

variable "ssh_key_name" {}

variable "release_label" {}

# IAM

variable "vhs_archive_manifest_full_access_policy_json" {}
variable "vhs_archive_manifest_read_policy_json" {}

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

variable "use_encryption_key_policy" {}

# Container images

variable "nginx_image" {}

# Configuration

variable "replica_primary_bucket_name" {}
variable "replica_glacier_bucket_name" {}

variable "vhs_archive_manifest_bucket_name" {}
variable "vhs_archive_manifest_table_name" {}
variable "static_content_bucket_name" {}

variable "bagger_mets_bucket_name" {}
variable "bagger_read_mets_from_fileshare" {}
variable "bagger_working_directory" {}
variable "bagger_current_preservation_bucket" {}
variable "bagger_dlcs_source_bucket" {}
variable "bagger_dlcs_entry" {}
variable "bagger_dlcs_customer_id" {}
variable "bagger_dlcs_space" {}
variable "bagger_dds_asset_prefix" {}
variable "bagger_ingest_table" {}
variable "bagger_ingest_table_arn" {}

variable "cognito_storage_api_identifier" {}
variable "cognito_user_pool_arn" {}

variable "alarm_topic_arn" {}

variable "replicas_table_arn" {}
variable "replicas_table_name" {}

# trigger_bag_ingest

variable "archive_oauth_details_enc" {}

variable "bag_paths" {
  default = "b22454408.zip"
}

variable "ingests_table_name" {}
variable "ingests_table_arn" {}

variable "workflow_bucket_name" {}

# bagger buckets

variable "s3_bagger_drop_arn" {}
variable "s3_bagger_drop_mets_only_arn" {}
variable "s3_bagger_errors_arn" {}

variable "s3_bagger_drop_name" {}
variable "s3_bagger_drop_mets_only_name" {}
variable "s3_bagger_errors_name" {}
variable "s3_bagger_cache_name" {}

# versioner table

variable "versioner_versions_table_arn" {}
variable "versioner_versions_table_name" {}
variable "versioner_versions_table_index" {}

# Task counts

variable "desired_bagger_count" {
  default = 4
}

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

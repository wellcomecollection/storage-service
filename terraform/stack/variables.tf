variable "namespace" {}

variable "domain_name" {}

variable "lambda_error_alarm_arn" {}
variable "dlq_alarm_arn" {}

variable "infra_bucket" {}
variable "current_account_id" {}

variable "ssh_key_name" {}
variable "instance_type" {}

# IAM

variable "vhs_archive_manifest_full_access_policy_json" {}
variable "vhs_archive_manifest_read_policy_json" {}

# Security groups

variable "service_egress_security_group_id" {}
variable "interservice_security_group_id" {}

# Network

variable "controlled_access_cidr_ingress" {
  type = "list"
}

variable "public_subnets" {
  type = "list"
}

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

variable "archivist_image" {}
variable "bags_image" {}
variable "bags_api_image" {}
variable "notifier_image" {}
variable "ingests_image" {}
variable "ingests_api_image" {}
variable "bagger_image" {}
variable "nginx_image" {}

# Configuration

variable "archive_bucket_name" {}
variable "vhs_archive_manifest_bucket_name" {}
variable "vhs_archive_manifest_table_name" {}
variable "static_content_bucket_name" {}

variable "bagger_mets_bucket_name" {}
variable "bagger_read_mets_from_fileshare" {}
variable "bagger_working_directory" {}
variable "bagger_drop_bucket_name" {}
variable "bagger_drop_bucket_name_mets_only" {}
variable "bagger_drop_bucket_name_errors" {}
variable "bagger_current_preservation_bucket" {}
variable "bagger_dlcs_source_bucket" {}
variable "bagger_dlcs_entry" {}
variable "bagger_dlcs_api_key" {}
variable "bagger_dlcs_api_secret" {}
variable "bagger_dlcs_customer_id" {}
variable "bagger_dlcs_space" {}
variable "bagger_dds_api_secret" {}
variable "bagger_dds_api_key" {}
variable "bagger_dds_asset_prefix" {}
variable "ingest_drop_bucket_name" {}

variable "cognito_storage_api_identifier" {}
variable "cognito_user_pool_arn" {}

variable "alarm_topic_arn" {}

# trigger_bag_ingest

variable "account_id" {}
variable "ingest_bucket_name" {}
variable "archive_oauth_details_enc" {}

variable "bag_paths" {
  default = "b22454408.zip"
}

variable "ingests_table_name" {}
variable "ingests_table_arn" {}

variable "workflow_bucket_name" {}

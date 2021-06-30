variable "namespace" {
  type = string
}

variable "api_url" {
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

variable "azure_container_name" {
  description = "The Azure container to use for replication.  If this value is null, the storage service will not replicate to Azure."
  type        = string
}

variable "azure_ssm_parameter_base" {
  description = "Prefix for the Secrets Manager secrets 'read_write_sas_url' and 'read_only_sas_url' that give access to Azure."
  type        = string
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

variable "upload_bucket_arns" {
  description = "ARNs of the S3 buckets where new bags will be uploaded"
  type        = list(string)

  validation {
    condition = (
      length(var.upload_bucket_arns) > 0
    )
    error_message = "There must be at least one bucket where you upload bags."
  }
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

variable "es_bags_index_name" {
  type = string
}

variable "es_ingests_index_name" {
  type = string
}

variable "es_files_index_name" {
  type = string
}

variable "indexer_host_secrets" {
  type = map(string)
}

variable "bag_indexer_secrets" {
  type = map(string)
}

variable "ingests_indexer_secrets" {
  type = map(string)
}

variable "file_indexer_secrets" {
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

variable "logging_container" {
  description = "Specifies container used for logging within applications."

  type = object({
    container_registry = string
    container_name     = string
    container_tag      = string
  })
}

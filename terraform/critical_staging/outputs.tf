output "ingests_table_name" {
  value = module.critical.ingests_table_name
}

output "ingests_table_arn" {
  value = module.critical.ingests_table_arn
}

output "replicas_table_arn" {
  value = module.critical.replicas_table_arn
}

output "replicas_table_name" {
  value = module.critical.replicas_table_name
}

# Replica buckets

output "replica_primary_bucket_name" {
  value = module.critical.replica_primary_bucket_name
}

output "replica_glacier_bucket_name" {
  value = module.critical.replica_glacier_bucket_name
}

output "replica_azure_container_name" {
  value = module.critical.replica_azure_container_name
}

# Static bucket

output "static_content_bucket_name" {
  value = module.critical.static_content_bucket_name
}

# Storage manifests VHS

output "vhs_manifests_bucket_name" {
  value = module.critical.vhs_manifests_bucket_name
}

output "vhs_manifests_table_name" {
  value = module.critical.vhs_manifests_table_name
}

# These two outputs aren't actually sensitive, but they're verbose and clutter
# up the CLI output.  Adding sensitive = true hides them from the CLI output.
# See https://www.terraform.io/docs/configuration/outputs.html
output "vhs_manifests_readonly_policy" {
  value     = module.critical.vhs_manifests_readonly_policy
  sensitive = true
}

output "vhs_manifests_readwrite_policy" {
  value     = module.critical.vhs_manifests_readwrite_policy
  sensitive = true
}

#

output "versions_table_arn" {
  value = module.critical.versions_table_arn
}

output "versions_table_name" {
  value = module.critical.versions_table_name
}

output "versions_table_index" {
  value = module.critical.versions_table_index
}

# Azure containers

output "azure_has_immutability_policy" {
  value = module.critical.azure_has_immutability_policy
}

output "azure_has_legal_hold" {
  value = module.critical.azure_has_legal_hold
}

# Storage manifests VHS backfill

output "vhs_manifests_bucket_name_backfill" {
  value = module.critical.vhs_manifests_bucket_name_backfill
}

output "vhs_manifests_table_name_backfill" {
  value = module.critical.vhs_manifests_table_name_backfill
}

output "vhs_manifests_readonly_policy_backfill" {
  value = module.critical.vhs_manifests_readonly_policy_backfill
}

output "vhs_manifests_readwrite_policy_backfill" {
  value = module.critical.vhs_manifests_readwrite_policy_backfill
}

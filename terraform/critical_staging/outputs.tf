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

#

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

output "vhs_manifests_readonly_policy" {
  value = module.critical.vhs_manifests_readonly_policy
}

output "vhs_manifests_readwrite_policy" {
  value = module.critical.vhs_manifests_readwrite_policy
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


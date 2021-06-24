# Storage manifests VHS

output "vhs_manifests_bucket_name" {
  value = module.metadata_stores.vhs_manifests_bucket_name
}

output "vhs_manifests_bucket_arn" {
  value = module.metadata_stores.vhs_manifests_bucket_arn
}

output "vhs_manifests_table_name" {
  value = module.metadata_stores.vhs_manifests_table_name
}

output "vhs_manifests_table_arn" {
  value = module.metadata_stores.vhs_manifests_table_arn
}

output "vhs_manifests_readonly_policy" {
  value = module.metadata_stores.vhs_manifests_readonly_policy
}

output "vhs_manifests_readwrite_policy" {
  value = module.metadata_stores.vhs_manifests_readwrite_policy
}

# Ingests table

output "ingests_table_name" {
  value = module.metadata_stores.ingests_table_name
}

output "ingests_table_arn" {
  value = module.metadata_stores.ingests_table_arn
}

# Replica buckets

output "replica_primary_bucket_name" {
  value = aws_s3_bucket.replica_primary.bucket
}

output "replica_primary_bucket_arn" {
  value = aws_s3_bucket.replica_primary.arn
}

output "replica_glacier_bucket_name" {
  value = aws_s3_bucket.replica_glacier.bucket
}

output "replica_glacier_bucket_arn" {
  value = aws_s3_bucket.replica_glacier.arn
}

output "replica_azure_container_name" {
  value = azurerm_storage_container.container.name
}

# Replicas table

output "replicas_table_arn" {
  value = module.metadata_stores.replicas_table_arn
}

output "replicas_table_name" {
  value = module.metadata_stores.replicas_table_name
}

# Versions table

output "versions_table_arn" {
  value = module.metadata_stores.versions_table_arn
}

output "versions_table_name" {
  value = module.metadata_stores.versions_table_name
}

output "versions_table_index" {
  value = module.metadata_stores.versions_table_index
}

# Azure containers

output "azure_has_immutability_policy" {
  value = azurerm_storage_container.container.has_immutability_policy
}

output "azure_has_legal_hold" {
  value = azurerm_storage_container.container.has_legal_hold
}

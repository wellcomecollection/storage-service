# Storage manifests VHS

output "vhs_manifests_bucket_name" {
  value = module.vhs_manifests.bucket_name
}

output "vhs_manifests_bucket_arn" {
  value = module.vhs_manifests.bucket_arn
}

output "vhs_manifests_table_name" {
  value = module.vhs_manifests.table_name
}

output "vhs_manifests_table_arn" {
  value = module.vhs_manifests.table_arn
}

output "vhs_manifests_readonly_policy" {
  value = module.vhs_manifests.read_policy
}

output "vhs_manifests_readwrite_policy" {
  value = module.vhs_manifests.full_access_policy
}

# Ingests table

output "ingests_table_name" {
  value = aws_dynamodb_table.ingests.name
}

output "ingests_table_arn" {
  value = aws_dynamodb_table.ingests.arn
}

# Replicas table

output "replicas_table_arn" {
  value = aws_dynamodb_table.replicas_table.arn
}

output "replicas_table_name" {
  value = aws_dynamodb_table.replicas_table.name
}

# Versions table

output "versions_table_arn" {
  value = aws_dynamodb_table.versioner_versions_table.arn
}

output "versions_table_name" {
  value = aws_dynamodb_table.versioner_versions_table.name
}

output "versions_table_index" {
  value = local.versioner_versions_table_index
}

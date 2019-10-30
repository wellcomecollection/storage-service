output "s3_bagger_drop_arn" {
  value = "${module.critical.s3_bagger_drop_arn}"
}

output "s3_bagger_errors_arn" {
  value = "${module.critical.s3_bagger_errors_arn}"
}

output "s3_bagger_drop_mets_only_arn" {
  value = "${module.critical.s3_bagger_drop_mets_only_arn}"
}

output "s3_bagger_drop_name" {
  value = "${module.critical.s3_bagger_drop_name}"
}

output "s3_bagger_errors_name" {
  value = "${module.critical.s3_bagger_errors_name}"
}

output "s3_bagger_drop_mets_only_name" {
  value = "${module.critical.s3_bagger_drop_mets_only_name}"
}

output "ingests_table_name" {
  value = "${module.critical.ingests_table_name}"
}

output "ingests_table_arn" {
  value = "${module.critical.ingests_table_arn}"
}

output "replicas_table_arn" {
  value = "${module.critical.replicas_table_arn}"
}

output "replicas_table_name" {
  value = "${module.critical.replicas_table_name}"
}

output "use_encryption_key_policy" {
  value = "${module.critical.use_encryption_key_policy}"
}

# Ingests drop bucket

output "ingests_drop_bucket_name" {
  value = "${module.critical.ingests_drop_bucket_name}"
}

output "ingests_drop_bucket_arn" {
  value = "${module.critical.ingests_drop_bucket_arn}"
}

# Replica buckets

output "replica_primary_bucket_name" {
  value = "${module.critical.replica_primary_bucket_name}"
}

output "replica_ireland_bucket_name" {
  value = "${module.critical.replica_ireland_bucket_name}"
}

#

output "static_content_bucket_name" {
  value = "${module.critical.static_content_bucket_name}"
}

output "manifests_bucket_name" {
  value = "${module.critical.manifests_bucket_name}"
}

output "manifests_table_name" {
  value = "${module.critical.manifests_table_name}"
}

output "manifests_read_policy" {
  value = "${module.critical.manifests_read_policy}"
}

output "manifests_full_access_policy" {
  value = "${module.critical.manifests_full_access_policy}"
}

output "s3_bagger_cache_name" {
  value = "${module.critical.s3_bagger_cache_name}"
}

output "versions_table_arn" {
  value = "${module.critical.versions_table_arn}"
}

output "versions_table_name" {
  value = "${module.critical.versions_table_name}"
}

output "versions_table_index" {
  value = "${module.critical.versions_table_index}"
}

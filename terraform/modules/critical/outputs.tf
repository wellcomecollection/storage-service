output "manifests_dynamodb_update_policy" {
  value = "${module.vhs_manifests.dynamodb_update_policy}"
}

output "manifests_bucket_name" {
  value = "${module.vhs_manifests.bucket_name}"
}

output "manifests_read_policy" {
  value = "${module.vhs_manifests.read_policy}"
}

output "manifests_full_access_policy" {
  value = "${module.vhs_manifests.full_access_policy}"
}

output "manifests_table_name" {
  value = "${module.vhs_manifests.table_name}"
}

output "ingests_table_name" {
  value = "${aws_dynamodb_table.ingests.name}"
}

output "ingests_table_arn" {
  value = "${aws_dynamodb_table.ingests.arn}"
}

output "bag_id_lookup_table_name" {
  value = "${aws_dynamodb_table.bag_id_lookup.name}"
}

output "bag_id_lookup_table_arn" {
  value = "${aws_dynamodb_table.bag_id_lookup.arn}"
}

output "use_encryption_key_policy" {
  value = "${module.kms_key.use_encryption_key_policy}"
}

output "static_content_bucket_name" {
  value = "${aws_s3_bucket.static_content.bucket}"
}

output "ingest_drop_bucket_name" {
  value = "${aws_s3_bucket.ingests_drop.bucket}"
}

output "archive_bucket_name" {
  value = "${aws_s3_bucket.archive.bucket}"
}

output "access_bucket_name" {
  value = "${aws_s3_bucket.access.bucket}"
}

output "s3_bagger_drop_arn" {
  value = "${aws_s3_bucket.bagger_drop.arn}"
}

output "s3_bagger_drop_mets_only_arn" {
  value = "${aws_s3_bucket.bagger_drop_mets_only.arn}"
}

output "s3_bagger_errors_arn" {
  value = "${aws_s3_bucket.bagger_errors.arn}"
}

output "s3_bagger_drop_name" {
  value = "${aws_s3_bucket.bagger_drop.id}"
}

output "s3_bagger_drop_mets_only_name" {
  value = "${aws_s3_bucket.bagger_drop_mets_only.id}"
}

output "s3_bagger_errors_name" {
  value = "${aws_s3_bucket.bagger_errors.id}"
}

output "s3_bagger_cache_name" {
  value = "${aws_s3_bucket.bagger_cache.id}"
}

output "replicas_table_arn" {
  value = "${aws_dynamodb_table.replicas_table.arn}"
}

output "replicas_table_name" {
  value = "${aws_dynamodb_table.replicas_table.name}"
}

output "versions_table_arn" {
  value = "${aws_dynamodb_table.versioner_versions_table.arn}"
}

output "versions_table_name" {
  value = "${aws_dynamodb_table.versioner_versions_table.name}"
}

output "versions_table_index" {
  value = "${local.versioner_versions_table_index}"
}

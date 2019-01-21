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

output "ingests_table_progress_index_name" {
  value = "${local.gsi_name}"
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

output "infra_bucket_name" {
  value = "${aws_s3_bucket.infra.bucket}"
}

output "service_egress_sg_id" {
  value = "${aws_security_group.service_egress.id}"
}

output "interservice_sg_id" {
  value = "${aws_security_group.interservice.id}"
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
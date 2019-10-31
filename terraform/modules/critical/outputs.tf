# Storage manifests VHS

output "vhs_manifests_bucket_name" {
  value = "${module.vhs_manifests.bucket_name}"
}

output "vhs_manifests_bucket_arn" {
  value = "${module.vhs_manifests.bucket_arn}"
}

output "vhs_manifests_table_name" {
  value = "${module.vhs_manifests.table_name}"
}

output "vhs_manifests_table_arn" {
  value = "${module.vhs_manifests.table_arn}"
}

output "vhs_manifests_readonly_policy" {
  value = "${module.vhs_manifests.read_policy}"
}

output "vhs_manifests_readwrite_policy" {
  value = "${module.vhs_manifests.full_access_policy}"
}

#

output "ingests_table_name" {
  value = "${aws_dynamodb_table.ingests.name}"
}

output "ingests_table_arn" {
  value = "${aws_dynamodb_table.ingests.arn}"
}

output "static_content_bucket_name" {
  value = "${aws_s3_bucket.static_content.bucket}"
}

# Replica buckets

output "replica_primary_bucket_name" {
  value = "${aws_s3_bucket.replica_primary.bucket}"
}

output "replica_primary_bucket_arn" {
  value = "${aws_s3_bucket.replica_primary.arn}"
}

output "replica_glacier_bucket_name" {
  value = "${aws_s3_bucket.replica_glacier.bucket}"
}

output "replica_glacier_bucket_arn" {
  value = "${aws_s3_bucket.replica_glacier.arn}"
}

# DynamoDB tables

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

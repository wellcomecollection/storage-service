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

output "repo_url_archivist" {
  value = "${module.ecr_archivist.repository_url}"
}

output "repo_url_bags" {
  value = "${module.ecr_bags.repository_url}"
}

output "repo_url_bags_api" {
  value = "${module.ecr_bags_api.repository_url}"
}

output "repo_url_ingests" {
  value = "${module.ecr_ingests.repository_url}"
}

output "repo_url_ingests_api" {
  value = "${module.ecr_ingests_api.repository_url}"
}

output "repo_url_notifier" {
  value = "${module.ecr_notifier.repository_url}"
}

output "repo_url_bag_replicator" {
  value = "${module.ecr_bag_replicator.repository_url}"
}

output "repo_url_bagger" {
  value = "${module.ecr_bagger.repository_url}"
}

output "repo_url_nginx_api_gw" {
  value = "${module.nginx_api_gw.repository_url}"
}

resource "aws_api_gateway_base_path_mapping" "mapping-colbert" {
  api_id      = "${module.stack-colbert.api_gateway_id}"
  domain_name = "${local.staging_domain_name}"
  base_path   = "storage"
}

module "stack-colbert" {
  source = "stack"

  namespace = "${local.namespace}-colbert"

  api_url          = "${local.staging_api_url}"
  domain_name      = "${local.staging_domain_name}"
  cert_domain_name = "${local.cert_domain_name}"

  vpc_id   = "${local.vpc_id}"
  vpc_cidr = "${local.vpc_cidr}"

  private_subnets = "${local.private_subnets}"

  ssh_key_name  = "${var.key_name}"
  instance_type = "i3.2xlarge"
  infra_bucket  = "${aws_s3_bucket.infra.id}"

  controlled_access_cidr_ingress = ["${local.admin_cidr_ingress}"]

  current_account_id     = "${data.aws_caller_identity.current.account_id}"
  lambda_error_alarm_arn = "${local.lambda_error_alarm_arn}"
  dlq_alarm_arn          = "${local.dlq_alarm_arn}"

  cognito_user_pool_arn          = "${local.cognito_user_pool_arn}"
  cognito_storage_api_identifier = "${local.cognito_storage_api_identifier}"

  release_label = "stage"
  nginx_image   = "${local.nginx_image}"

  archive_bucket_name              = "${module.critical-staging.archive_bucket_name}"
  archivist_queue_parallelism      = "4"
  access_bucket_name               = "${module.critical-staging.access_bucket_name}"
  static_content_bucket_name       = "${module.critical-staging.static_content_bucket_name}"
  vhs_archive_manifest_table_name  = "${module.critical-staging.manifests_table_name}"
  vhs_archive_manifest_bucket_name = "${module.critical-staging.manifests_bucket_name}"

  bagger_dlcs_space                  = "${local.bagger_dlcs_space}"
  bagger_working_directory           = "${local.bagger_working_directory}"
  bagger_dlcs_entry                  = "${local.bagger_dlcs_entry}"
  bagger_mets_bucket_name            = "${local.bagger_mets_bucket_name}"
  bagger_dlcs_customer_id            = "${local.bagger_dlcs_customer_id}"
  bagger_read_mets_from_fileshare    = "${local.bagger_read_mets_from_fileshare}"
  bagger_dlcs_source_bucket          = "${local.bagger_dlcs_source_bucket}"
  bagger_current_preservation_bucket = "${local.bagger_current_preservation_bucket}"
  bagger_dds_asset_prefix            = "${local.bagger_dds_asset_prefix}"
  bagger_progress_table              = "${local.bagger_progress_table_stage}"
  bagger_progress_table_arn          = "${local.bagger_progress_table_stage_arn}"

  s3_bagger_drop_arn           = "${module.critical-staging.s3_bagger_drop_arn}"
  s3_bagger_errors_arn         = "${module.critical-staging.s3_bagger_errors_arn}"
  s3_bagger_drop_mets_only_arn = "${module.critical-staging.s3_bagger_drop_mets_only_arn}"

  s3_bagger_drop_name           = "${module.critical-staging.s3_bagger_drop_name}"
  s3_bagger_errors_name         = "${module.critical-staging.s3_bagger_errors_name}"
  s3_bagger_drop_mets_only_name = "${module.critical-staging.s3_bagger_drop_mets_only_name}"

  vhs_archive_manifest_full_access_policy_json = "${module.critical-staging.manifests_full_access_policy}"
  vhs_archive_manifest_read_policy_json        = "${module.critical-staging.manifests_read_policy}"

  alarm_topic_arn = "${local.gateway_server_error_alarm_arn}"

  ingests_table_name = "${module.critical-staging.ingests_table_name}"
  ingests_table_arn  = "${module.critical-staging.ingests_table_arn}"

  ingests_table_progress_index_name = "${module.critical-staging.ingests_table_progress_index_name}"

  ingest_bucket_name = "${module.critical-staging.ingest_drop_bucket_name}"

  archive_oauth_details_enc = "${var.archive_oauth_details_enc}"
  account_id                = "${local.account_id}"

  use_encryption_key_policy = "${module.critical-staging.use_encryption_key_policy}"

  workflow_bucket_name = "${local.workflow_bucket_name}"

  ingest_drop_bucket_name = "${module.critical-staging.ingest_drop_bucket_name}"
}

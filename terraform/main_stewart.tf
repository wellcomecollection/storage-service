resource "aws_api_gateway_base_path_mapping" "mapping-stewart" {
  api_id      = "${module.stack-stewart.api_gateway_id}"
  domain_name = "${local.domain_name}"
  base_path   = "storage"
}

module "stack-stewart" {
  source = "stack"

  namespace = "${local.namespace}-stewart"

  api_url          = "${local.api_url}"
  domain_name      = "${local.domain_name}"
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

  release_label = "prod"
  nginx_image   = "${local.nginx_image}"

  archive_bucket_name              = "${module.critical.archive_bucket_name}"
  archivist_queue_parallelism      = "4"
  access_bucket_name               = "${module.critical.access_bucket_name}"
  static_content_bucket_name       = "${module.critical.static_content_bucket_name}"
  vhs_archive_manifest_table_name  = "${module.critical.manifests_table_name}"
  vhs_archive_manifest_bucket_name = "${module.critical.manifests_bucket_name}"

  bagger_dlcs_space                  = "${local.bagger_dlcs_space}"
  bagger_working_directory           = "${local.bagger_working_directory}"
  bagger_dlcs_entry                  = "${local.bagger_dlcs_entry}"
  bagger_mets_bucket_name            = "${local.bagger_mets_bucket_name}"
  bagger_dlcs_customer_id            = "${local.bagger_dlcs_customer_id}"
  bagger_read_mets_from_fileshare    = "${local.bagger_read_mets_from_fileshare}"
  bagger_dlcs_source_bucket          = "${local.bagger_dlcs_source_bucket}"
  bagger_current_preservation_bucket = "${local.bagger_current_preservation_bucket}"
  bagger_dds_asset_prefix            = "${local.bagger_dds_asset_prefix}"
  bagger_progress_table              = "${local.bagger_progress_table}"
  bagger_progress_table_arn          = "${local.bagger_progress_table_arn}"

  s3_bagger_drop_arn           = "${module.critical.s3_bagger_drop_arn}"
  s3_bagger_errors_arn         = "${module.critical.s3_bagger_errors_arn}"
  s3_bagger_drop_mets_only_arn = "${module.critical.s3_bagger_drop_mets_only_arn}"

  s3_bagger_drop_name           = "${module.critical.s3_bagger_drop_name}"
  s3_bagger_errors_name         = "${module.critical.s3_bagger_errors_name}"
  s3_bagger_drop_mets_only_name = "${module.critical.s3_bagger_drop_mets_only_name}"

  vhs_archive_manifest_full_access_policy_json = "${module.critical.manifests_full_access_policy}"
  vhs_archive_manifest_read_policy_json        = "${module.critical.manifests_read_policy}"

  alarm_topic_arn = "${local.gateway_server_error_alarm_arn}"

  ingests_table_name = "${module.critical.ingests_table_name}"
  ingests_table_arn  = "${module.critical.ingests_table_arn}"

  ingests_table_progress_index_name = "${module.critical.ingests_table_progress_index_name}"

  ingest_bucket_name = "${module.critical.ingest_drop_bucket_name}"

  archive_oauth_details_enc = "${var.archive_oauth_details_enc}"
  account_id                = "${local.account_id}"

  use_encryption_key_policy = "${module.critical.use_encryption_key_policy}"

  workflow_bucket_name = "${local.workflow_bucket_name}"

  ingest_drop_bucket_name = "${module.critical.ingest_drop_bucket_name}"
}

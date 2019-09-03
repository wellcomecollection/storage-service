resource "aws_api_gateway_base_path_mapping" "mapping_staging" {
  api_id      = "${module.stack_staging.api_gateway_id}"
  domain_name = "${local.staging_domain_name}"
  base_path   = "storage"
}

module "stack_staging" {
  source = "../stack"

  namespace = "${local.namespace}-staging"

  api_url          = "${local.staging_api_url}"
  domain_name      = "${local.staging_domain_name}"
  cert_domain_name = "${local.cert_domain_name}"

  desired_bagger_count  = 3
  desired_ec2_instances = 1

  vpc_id   = "${local.vpc_id}"
  vpc_cidr = "${local.vpc_cidr}"

  private_subnets = "${local.private_subnets}"

  ssh_key_name = "${local.key_name}"
  infra_bucket = "${local.infra_bucket}"

  lambda_error_alarm_arn = "${local.lambda_error_alarm_arn}"
  dlq_alarm_arn          = "${local.dlq_alarm_arn}"

  cognito_user_pool_arn          = "${local.cognito_user_pool_arn}"
  cognito_storage_api_identifier = "${local.cognito_storage_api_identifier}"

  release_label = "stage"
  nginx_image   = "${local.nginx_image}"

  archive_bucket_name              = "${data.terraform_remote_state.critical_staging.archive_bucket_name}"
  access_bucket_name               = "${data.terraform_remote_state.critical_staging.access_bucket_name}"
  static_content_bucket_name       = "${data.terraform_remote_state.critical_staging.static_content_bucket_name}"
  vhs_archive_manifest_table_name  = "${data.terraform_remote_state.critical_staging.manifests_table_name}"
  vhs_archive_manifest_bucket_name = "${data.terraform_remote_state.critical_staging.manifests_bucket_name}"

  bagger_dlcs_space                  = "${local.bagger_dlcs_space}"
  bagger_working_directory           = "${local.bagger_working_directory}"
  bagger_dlcs_entry                  = "${local.bagger_dlcs_entry}"
  bagger_mets_bucket_name            = "${local.bagger_mets_bucket_name}"
  bagger_dlcs_customer_id            = "${local.bagger_dlcs_customer_id}"
  bagger_read_mets_from_fileshare    = "${local.bagger_read_mets_from_fileshare}"
  bagger_dlcs_source_bucket          = "${local.bagger_dlcs_source_bucket}"
  bagger_current_preservation_bucket = "${local.bagger_current_preservation_bucket}"
  bagger_dds_asset_prefix            = "${local.bagger_dds_asset_prefix}"
  bagger_ingest_table                = "${local.bagger_ingest_table_stage}"
  bagger_ingest_table_arn            = "${local.bagger_ingest_table_stage_arn}"

  versioner_versions_table_arn   = "${data.terraform_remote_state.critical_staging.versions_table_arn}"
  versioner_versions_table_name  = "${data.terraform_remote_state.critical_staging.versions_table_name}"
  versioner_versions_table_index = "${data.terraform_remote_state.critical_staging.versions_table_index}"

  s3_bagger_drop_arn           = "${data.terraform_remote_state.critical_staging.s3_bagger_drop_arn}"
  s3_bagger_errors_arn         = "${data.terraform_remote_state.critical_staging.s3_bagger_errors_arn}"
  s3_bagger_drop_mets_only_arn = "${data.terraform_remote_state.critical_staging.s3_bagger_drop_mets_only_arn}"

  s3_bagger_drop_name           = "${data.terraform_remote_state.critical_staging.s3_bagger_drop_name}"
  s3_bagger_errors_name         = "${data.terraform_remote_state.critical_staging.s3_bagger_errors_name}"
  s3_bagger_drop_mets_only_name = "${data.terraform_remote_state.critical_staging.s3_bagger_drop_mets_only_name}"
  s3_bagger_cache_name          = "${data.terraform_remote_state.critical_prod.s3_bagger_cache_name}"

  vhs_archive_manifest_full_access_policy_json = "${data.terraform_remote_state.critical_staging.manifests_full_access_policy}"
  vhs_archive_manifest_read_policy_json        = "${data.terraform_remote_state.critical_staging.manifests_read_policy}"

  alarm_topic_arn = "${local.gateway_server_error_alarm_arn}"

  ingests_table_name = "${data.terraform_remote_state.critical_staging.ingests_table_name}"
  ingests_table_arn  = "${data.terraform_remote_state.critical_staging.ingests_table_arn}"

  bag_id_lookup_table_name = "${data.terraform_remote_state.critical_staging.bag_id_lookup_table_name}"
  bag_id_lookup_table_arn  = "${data.terraform_remote_state.critical_staging.bag_id_lookup_table_arn}"

  replicas_table_arn  = "${data.terraform_remote_state.critical_staging.replicas_table_arn}"
  replicas_table_name = "${data.terraform_remote_state.critical_staging.replicas_table_name}"

  ingest_bucket_name = "${data.terraform_remote_state.critical_staging.ingest_drop_bucket_name}"

  archive_oauth_details_enc = "${local.archive_oauth_details_enc}"

  use_encryption_key_policy = "${data.terraform_remote_state.critical_staging.use_encryption_key_policy}"

  workflow_bucket_name = "${local.workflow_staging_bucket_name}"

  ingest_drop_bucket_name = "${data.terraform_remote_state.critical_staging.ingest_drop_bucket_name}"

  archivematica_ingests_bucket = "${local.archivematica_ingests_bucket}"
}

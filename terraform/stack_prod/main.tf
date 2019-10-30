resource "aws_api_gateway_base_path_mapping" "mapping_prod" {
  api_id      = "${module.stack_prod.api_gateway_id}"
  domain_name = "${local.domain_name}"
  base_path   = "storage"
}

module "stack_prod" {
  source = "../modules/stack"

  namespace = "${local.namespace}-prod"

  api_url          = "${local.api_url}"
  domain_name      = "${local.domain_name}"
  cert_domain_name = "${local.cert_domain_name}"

  desired_bagger_count  = 3
  desired_ec2_instances = 1

  min_capacity = 1
  max_capacity = 10

  vpc_id   = "${local.vpc_id}"
  vpc_cidr = "${local.vpc_cidr}"

  private_subnets = "${local.private_subnets}"

  ssh_key_name = "${local.key_name}"
  infra_bucket = "${local.infra_bucket}"

  lambda_error_alarm_arn = "${local.lambda_error_alarm_arn}"
  dlq_alarm_arn          = "${local.dlq_alarm_arn}"

  cognito_user_pool_arn          = "${local.cognito_user_pool_arn}"
  cognito_storage_api_identifier = "${local.cognito_storage_api_identifier}"

  release_label = "prod"
  nginx_image   = "${local.nginx_image}"

  replica_primary_bucket_name = "${data.terraform_remote_state.critical_prod.replica_primary_bucket_name}"
  replica_ireland_bucket_name = "${data.terraform_remote_state.critical_prod.replica_ireland_bucket_name}"

  static_content_bucket_name       = "${data.terraform_remote_state.critical_prod.static_content_bucket_name}"
  vhs_archive_manifest_table_name  = "${data.terraform_remote_state.critical_prod.manifests_table_name}"
  vhs_archive_manifest_bucket_name = "${data.terraform_remote_state.critical_prod.manifests_bucket_name}"

  bagger_dlcs_space                  = "${local.bagger_dlcs_space}"
  bagger_working_directory           = "${local.bagger_working_directory}"
  bagger_dlcs_entry                  = "${local.bagger_dlcs_entry}"
  bagger_mets_bucket_name            = "${local.bagger_mets_bucket_name}"
  bagger_dlcs_customer_id            = "${local.bagger_dlcs_customer_id}"
  bagger_read_mets_from_fileshare    = "${local.bagger_read_mets_from_fileshare}"
  bagger_dlcs_source_bucket          = "${local.bagger_dlcs_source_bucket}"
  bagger_current_preservation_bucket = "${local.bagger_current_preservation_bucket}"
  bagger_dds_asset_prefix            = "${local.bagger_dds_asset_prefix}"
  bagger_ingest_table                = "${local.bagger_ingest_table}"
  bagger_ingest_table_arn            = "${local.bagger_ingest_table_arn}"

  versioner_versions_table_arn   = "${data.terraform_remote_state.critical_prod.versions_table_arn}"
  versioner_versions_table_name  = "${data.terraform_remote_state.critical_prod.versions_table_name}"
  versioner_versions_table_index = "${data.terraform_remote_state.critical_prod.versions_table_index}"

  s3_bagger_drop_arn           = "${data.terraform_remote_state.critical_prod.s3_bagger_drop_arn}"
  s3_bagger_errors_arn         = "${data.terraform_remote_state.critical_prod.s3_bagger_errors_arn}"
  s3_bagger_drop_mets_only_arn = "${data.terraform_remote_state.critical_prod.s3_bagger_drop_mets_only_arn}"

  s3_bagger_drop_name           = "${data.terraform_remote_state.critical_prod.s3_bagger_drop_name}"
  s3_bagger_errors_name         = "${data.terraform_remote_state.critical_prod.s3_bagger_errors_name}"
  s3_bagger_drop_mets_only_name = "${data.terraform_remote_state.critical_prod.s3_bagger_drop_mets_only_name}"
  s3_bagger_cache_name          = "${data.terraform_remote_state.critical_prod.s3_bagger_cache_name}"

  vhs_archive_manifest_full_access_policy_json = "${data.terraform_remote_state.critical_prod.manifests_full_access_policy}"
  vhs_archive_manifest_read_policy_json        = "${data.terraform_remote_state.critical_prod.manifests_read_policy}"

  alarm_topic_arn = "${local.gateway_server_error_alarm_arn}"

  ingests_table_name = "${data.terraform_remote_state.critical_prod.ingests_table_name}"
  ingests_table_arn  = "${data.terraform_remote_state.critical_prod.ingests_table_arn}"

  replicas_table_arn  = "${data.terraform_remote_state.critical_prod.replicas_table_arn}"
  replicas_table_name = "${data.terraform_remote_state.critical_prod.replicas_table_name}"

  archive_oauth_details_enc = "${local.archive_oauth_details_enc}"

  use_encryption_key_policy = "${data.terraform_remote_state.critical_prod.use_encryption_key_policy}"

  workflow_bucket_name = "${local.workflow_bucket_name}"

  archivematica_ingests_bucket = "${local.archivematica_ingests_bucket}"
}

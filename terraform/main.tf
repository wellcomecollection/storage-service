resource "aws_api_gateway_base_path_mapping" "mapping" {
  api_id      = "${module.stack.api_gateway_id}"
  domain_name = "${local.domain_name}"
  base_path   = "storage"
}

module "critical" {
  source = "critical"

  namespace  = "${local.namespace}"
  account_id = "${local.account_id}"

  vpc_id = "${local.vpc_id}"

  private_cidr_block_ids = ["${local.private_subnet_ids}"]
}

module "stack" {
  source = "stack"

  namespace = "${local.namespace}-030119"

  domain_name = "${local.domain_name}"

  vpc_id   = "${local.vpc_id}"
  vpc_cidr = "${local.vpc_cidr}"

  private_subnets = "${local.private_subnets}"
  public_subnets  = "${local.public_subnets}"

  ssh_key_name = "${var.key_name}"

  instance_type = "i3.2xlarge"

  infra_bucket = "${module.critical.infra_bucket_name}"

  controlled_access_cidr_ingress = ["${var.admin_cidr_ingress}"]

  current_account_id     = "${data.aws_caller_identity.current.account_id}"
  lambda_error_alarm_arn = "${local.lambda_error_alarm_arn}"
  dlq_alarm_arn          = "${local.dlq_alarm_arn}"

  service_egress_security_group_id = "${module.critical.service_egress_sg_id}"
  interservice_security_group_id   = "${module.critical.interservice_sg_id}"

  cognito_user_pool_arn          = "${local.cognito_user_pool_arn}"
  cognito_storage_api_identifier = "${local.cognito_storage_api_identifier}"

  bags_image           = "${local.bags_image}"
  ingests_image        = "${local.ingests_image}"
  ingests_api_image    = "${local.ingests_api_image}"
  bags_api_image       = "${local.bags_api_image}"
  archivist_image      = "${local.archivist_image}"
  notifier_image       = "${local.notifier_image}"
  nginx_image          = "${local.nginx_image}"
  bagger_image         = "${local.bagger_image}"
  bag_replicator_image = "${local.bag_replicator_image}"

  archive_bucket_name              = "${module.critical.archive_bucket_name}"
  access_bucket_name               = "${module.critical.access_bucket_name}"
  static_content_bucket_name       = "${module.critical.static_content_bucket_name}"
  vhs_archive_manifest_table_name  = "${module.critical.manifests_table_name}"
  vhs_archive_manifest_bucket_name = "${module.critical.manifests_bucket_name}"

  bagger_dlcs_space                  = "${var.bagger_dlcs_space}"
  bagger_dds_api_key                 = "${var.bagger_dds_api_key}"
  bagger_working_directory           = "${var.bagger_working_directory}"
  bagger_dlcs_entry                  = "${var.bagger_dlcs_entry}"
  bagger_dlcs_api_key                = "${var.bagger_dlcs_api_key}"
  bagger_dlcs_api_secret             = "${var.bagger_dlcs_api_secret}"
  bagger_mets_bucket_name            = "${var.bagger_mets_bucket_name}"
  bagger_dlcs_customer_id            = "${var.bagger_dlcs_customer_id}"
  bagger_drop_bucket_name            = "${var.bagger_drop_bucket_name}"
  bagger_drop_bucket_name_errors     = "${var.bagger_drop_bucket_name_errors}"
  bagger_read_mets_from_fileshare    = "${var.bagger_read_mets_from_fileshare}"
  bagger_dlcs_source_bucket          = "${var.bagger_dlcs_source_bucket}"
  bagger_drop_bucket_name_mets_only  = "${var.bagger_drop_bucket_name_mets_only}"
  bagger_current_preservation_bucket = "${var.bagger_current_preservation_bucket}"
  bagger_dds_api_secret              = "${var.bagger_dds_api_secret}"
  bagger_dds_asset_prefix            = "${var.bagger_dds_asset_prefix}"

  vhs_archive_manifest_full_access_policy_json = "${module.critical.manifests_full_access_policy}"
  vhs_archive_manifest_read_policy_json        = "${module.critical.manifests_read_policy}"

  alarm_topic_arn = "${local.gateway_server_error_alarm_arn}"

  ingests_table_name = "${module.critical.ingests_table_name}"
  ingests_table_arn  = "${module.critical.ingests_table_arn}"
  ingest_bucket_name = "${module.critical.ingest_drop_bucket_name}"

  archive_oauth_details_enc = "${var.archive_oauth_details_enc}"
  account_id                = "${local.account_id}"

  use_encryption_key_policy = "${module.critical.use_encryption_key_policy}"

  workflow_bucket_name    = "${local.workflow_bucket_name}"
  ingest_drop_bucket_name = "${module.critical.ingest_drop_bucket_name}"
}

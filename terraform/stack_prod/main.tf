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

  min_capacity = 1
  max_capacity = 10

  vpc_id = "${local.vpc_id}"

  private_subnets = "${local.private_subnets}"

  dlq_alarm_arn = "${local.dlq_alarm_arn}"

  cognito_user_pool_arn          = "${local.cognito_user_pool_arn}"
  cognito_storage_api_identifier = "${local.cognito_storage_api_identifier}"

  release_label = "prod"
  nginx_image   = "${local.nginx_image}"

  replica_primary_bucket_name = "${data.terraform_remote_state.critical_prod.replica_primary_bucket_name}"
  replica_glacier_bucket_name = "${data.terraform_remote_state.critical_prod.replica_glacier_bucket_name}"

  static_content_bucket_name = "${data.terraform_remote_state.critical_prod.static_content_bucket_name}"

  vhs_manifests_bucket_name = "${data.terraform_remote_state.critical_prod.vhs_manifests_bucket_name}"
  vhs_manifests_table_name  = "${data.terraform_remote_state.critical_prod.vhs_manifests_table_name}"

  vhs_manifests_readonly_policy  = "${data.terraform_remote_state.critical_prod.vhs_manifests_readonly_policy}"
  vhs_manifests_readwrite_policy = "${data.terraform_remote_state.critical_prod.vhs_manifests_readwrite_policy}"

  versioner_versions_table_arn   = "${data.terraform_remote_state.critical_prod.versions_table_arn}"
  versioner_versions_table_name  = "${data.terraform_remote_state.critical_prod.versions_table_name}"
  versioner_versions_table_index = "${data.terraform_remote_state.critical_prod.versions_table_index}"

  alarm_topic_arn = "${local.gateway_server_error_alarm_arn}"

  ingests_table_name = "${data.terraform_remote_state.critical_prod.ingests_table_name}"
  ingests_table_arn  = "${data.terraform_remote_state.critical_prod.ingests_table_arn}"

  replicas_table_arn  = "${data.terraform_remote_state.critical_prod.replicas_table_arn}"
  replicas_table_name = "${data.terraform_remote_state.critical_prod.replicas_table_name}"

  workflow_bucket_name = "${local.workflow_bucket_name}"

  archivematica_ingests_bucket = "${local.archivematica_ingests_bucket}"
}

data "aws_subnet" "private_new" {
  count = "3"
  id    = "${element(local.private_subnets, count.index)}"
}

locals {
  namespace = "storage"

  private_subnet_ids = [
    "${data.aws_subnet.private_new.*.cidr_block}",
  ]

  domain_name = "storage.api-stage.wellcomecollection.org"
  cert_domain_name = "storage.api.wellcomecollection.org"

  vpc_id          = "${data.terraform_remote_state.infra_shared.storage_vpc_id}"
  private_subnets = "${data.terraform_remote_state.infra_shared.storage_vpc_private_subnets}"
  public_subnets  = "${data.terraform_remote_state.infra_shared.storage_vpc_public_subnets}"
  account_id      = "${data.aws_caller_identity.current.account_id}"
  vpc_cidr        = ["${data.terraform_remote_state.infra_shared.storage_cidr_block_vpc}"]

  cognito_user_pool_arn          = "${data.terraform_remote_state.infra_critical.cognito_user_pool_arn}"
  cognito_storage_api_identifier = "${data.terraform_remote_state.infra_critical.cognito_storage_api_identifier}"
  lambda_error_alarm_arn         = "${data.terraform_remote_state.infra_shared.lambda_error_alarm_arn}"
  dlq_alarm_arn                  = "${data.terraform_remote_state.infra_shared.dlq_alarm_arn}"

  archivist_image      = "${module.critical.repo_url_archivist}:${var.release_ids["archivist"]}"
  bags_image           = "${module.critical.repo_url_bags}:${var.release_ids["bags"]}"
  bags_api_image       = "${module.critical.repo_url_bags_api}:${var.release_ids["bags_api"]}"
  ingests_image        = "${module.critical.repo_url_ingests}:${var.release_ids["ingests"]}"
  ingests_api_image    = "${module.critical.repo_url_ingests_api}:${var.release_ids["ingests_api"]}"
  notifier_image       = "${module.critical.repo_url_notifier}:${var.release_ids["notifier"]}"
  bag_replicator_image = "${module.critical.repo_url_bag_replicator}:${var.release_ids["bag_replicator"]}"
  bagger_image         = "${module.critical.repo_url_bagger}:${var.release_ids["bagger"]}"
  nginx_api_gw_image   = "${module.critical.repo_url_nginx_api_gw}:${var.release_ids["nginx_api_gw"]}"

  nginx_image = "760097843905.dkr.ecr.eu-west-1.amazonaws.com/uk.ac.wellcome/nginx_api-gw:bad0dbfa548874938d16496e313b05adb71268b7"

  gateway_server_error_alarm_arn = "${data.terraform_remote_state.infra_shared.gateway_server_error_alarm_arn}"

  workflow_bucket_name = "wellcomecollection-workflow-export-bagit"
}

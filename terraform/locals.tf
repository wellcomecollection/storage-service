locals {
  namespace = "storage"

  private_subnet_ids = [
    "${data.aws_subnet.private_new.*.cidr_block}",
  ]

  api_url          = "https://api.wellcomecollection.org"
  domain_name      = "storage.api.wellcomecollection.org"
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

  archivist_image = "${data.aws_ssm_parameter.archivist_image.value}"
  bags_image = "${data.aws_ssm_parameter.bags_image.value}"
  bags_api_image = "${data.aws_ssm_parameter.bags_api_image.value}"
  ingests_image = "${data.aws_ssm_parameter.ingests_image.value}"
  ingests_api_image = "${data.aws_ssm_parameter.ingests_api_image.value}"
  notifier_image = "${data.aws_ssm_parameter.notifier_image.value}"
  bagger_image = "${data.aws_ssm_parameter.bagger_image.value}"
  bag_replicator_image = "${data.aws_ssm_parameter.bag_replicator_image.value}"

  nginx_image = "760097843905.dkr.ecr.eu-west-1.amazonaws.com/uk.ac.wellcome/nginx_api-gw:bad0dbfa548874938d16496e313b05adb71268b7"

  gateway_server_error_alarm_arn = "${data.terraform_remote_state.infra_shared.gateway_server_error_alarm_arn}"

  workflow_bucket_name = "wellcomecollection-workflow-export-bagit"
}
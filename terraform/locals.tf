locals {
  namespace = "storage"

  private_subnet_ids = [
    "${data.aws_subnet.private_new.*.cidr_block}",
  ]

  api_url     = "https://api.wellcomecollection.org"
  domain_name = "storage.api.wellcomecollection.org"

  staging_api_url     = "https://api-stage.wellcomecollection.org"
  staging_domain_name = "storage.api-stage.wellcomecollection.org"

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

  nginx_image = "760097843905.dkr.ecr.eu-west-1.amazonaws.com/uk.ac.wellcome/nginx_api-gw:bad0dbfa548874938d16496e313b05adb71268b7"

  gateway_server_error_alarm_arn = "${data.terraform_remote_state.infra_shared.gateway_server_error_alarm_arn}"

  service-wt-winnipeg = "${data.terraform_remote_state.infra_shared.service-wt-winnipeg}"
  service-pl-winslow  = "${data.terraform_remote_state.infra_shared.service-pl-winslow}"

  subnets_ids = [
    "${data.terraform_remote_state.infra_shared.storage_vpc_private_subnets[0]}",
    "${data.terraform_remote_state.infra_shared.storage_vpc_private_subnets[2]}",
  ]

  workflow_bucket_name = "wellcomecollection-workflow-export-bagit"

  admin_cidr_ingress = "195.143.129.128/25"

  bagger_progress_table     = "storage-migration-status"
  bagger_progress_table_arn = "arn:aws:dynamodb:eu-west-1:975596993436:table/storage-migration-status"

  bagger_progress_table_stage     = "storage-staging-migration-status"
  bagger_progress_table_stage_arn = "arn:aws:dynamodb:eu-west-1:975596993436:table/storage-staging-migration-status"

  goobi_task_role_arn = "arn:aws:iam::299497370133:role/goobi_task_role"
}

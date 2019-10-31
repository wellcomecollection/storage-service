locals {
  namespace = "storage"

  staging_api_url     = "https://api-stage.wellcomecollection.org"
  staging_domain_name = "storage.api-stage.wellcomecollection.org"

  vpc_id          = "${data.terraform_remote_state.infra_shared.storage_vpc_id}"
  vpc_cidr        = ["${data.terraform_remote_state.infra_shared.storage_cidr_block_vpc}"]
  private_subnets = "${data.terraform_remote_state.infra_shared.storage_vpc_private_subnets}"

  cert_domain_name = "storage.api.wellcomecollection.org"

  key_name = "wellcomedigitalstorage"

  dlq_alarm_arn = "${data.terraform_remote_state.infra_shared.dlq_alarm_arn}"

  cognito_user_pool_arn          = "${data.terraform_remote_state.infra_critical.cognito_user_pool_arn}"
  cognito_storage_api_identifier = "${data.terraform_remote_state.infra_critical.cognito_storage_api_identifier}"

  nginx_image = "760097843905.dkr.ecr.eu-west-1.amazonaws.com/uk.ac.wellcome/nginx_api-gw:bad0dbfa548874938d16496e313b05adb71268b7"

  gateway_server_error_alarm_arn = "${data.terraform_remote_state.infra_shared.gateway_server_error_alarm_arn}"

  workflow_staging_bucket_name = "wellcomecollection-workflow-export-bagit-stage"

  archivematica_ingests_bucket = "${data.terraform_remote_state.archivematica_infra.ingests_bucket}"

  subnets_ids = [
    "${data.terraform_remote_state.infra_shared.storage_vpc_private_subnets[0]}",
    "${data.terraform_remote_state.infra_shared.storage_vpc_private_subnets[2]}",
  ]

  service-pl-winslow = "${data.terraform_remote_state.infra_shared.service-pl-winslow}"
}

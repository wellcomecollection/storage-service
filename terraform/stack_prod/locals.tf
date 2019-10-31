locals {
  namespace = "storage"

  api_url     = "https://api.wellcomecollection.org"
  domain_name = "storage.api.wellcomecollection.org"

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

  workflow_bucket_name = "wellcomecollection-workflow-export-bagit"

  archivematica_ingests_bucket = "${data.terraform_remote_state.archivematica_infra.ingests_bucket}"

  bagger_ingest_table     = "storage-migration-status"
  bagger_ingest_table_arn = "arn:aws:dynamodb:eu-west-1:975596993436:table/storage-migration-status"

  subnets_ids = [
    "${data.terraform_remote_state.infra_shared.storage_vpc_private_subnets[0]}",
    "${data.terraform_remote_state.infra_shared.storage_vpc_private_subnets[2]}",
  ]

  service-wt-winnipeg = "${data.terraform_remote_state.infra_shared.service-wt-winnipeg}"
}

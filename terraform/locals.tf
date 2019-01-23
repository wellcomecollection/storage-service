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

  archivist_image      = "${data.aws_ssm_parameter.archivist_image.value}"
  bags_image           = "${data.aws_ssm_parameter.bags_image.value}"
  bags_api_image       = "${data.aws_ssm_parameter.bags_api_image.value}"
  ingests_image        = "${data.aws_ssm_parameter.ingests_image.value}"
  ingests_api_image    = "${data.aws_ssm_parameter.ingests_api_image.value}"
  notifier_image       = "${data.aws_ssm_parameter.notifier_image.value}"
  bagger_image         = "${data.aws_ssm_parameter.bagger_image.value}"
  bag_replicator_image = "${data.aws_ssm_parameter.bag_replicator_image.value}"

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

  bagger_progress_table = "storage-migration-status"

  //  key_name = "wellcomedigitalstorage"
  //  allowed_principles = ["arn:aws:iam::760097843905:root"]
  //
  //  # Bagger configuration
  //
  //  bagger_mets_bucket_name = "wellcomecollection-assets-workingstorage"
  //  bagger_read_mets_from_fileshare = "False"
  //  bagger_working_directory = "/tmp/_bagger"
  //  bagger_current_preservation_bucket = "wdl-preservica"
  //  bagger_dlcs_source_bucket = "dlcs-storage"
  //
  //  # DLCS config
  //  bagger_dlcs_entry = "https://api.dlcs.io/"
  //  bagger_dlcs_customer_id = "2"
  //  bagger_dlcs_space = "1"
  //
  //  # DLCS secrets
  //  bagger_dlcs_api_key = "${module.bagger_dlcs_api_key.ssm_param_value}"
  //  bagger_dlcs_api_secret = "${module.bagger_dlcs_api_secret.ssm_param_value}"
  //
  //  # DDS config
  //  bagger_dds_asset_prefix = "https://wellcomelibrary.org/service/asset/"
  //
  //  # DDS secrets
  //  bagger_dds_api_key = "${module.bagger_dds_api_key.ssm_param_value}"
  //  bagger_dds_api_secret = "${module.bagger_dds_api_secret.ssm_param_value}"
  //
  //  # AWS Secrets
  //  bagger_aws_access_key_id = "${module.bagger_aws_access_key_id.ssm_param_value}"
  //  bagger_aws_secret_access_key = "${module.bagger_aws_secret_access_key.ssm_param_value}"
  //
  //  # Storage OAuth
  //  archive_oauth_details_enc = "${module.archive_oauth_details_enc.ssm_param_value}"
}

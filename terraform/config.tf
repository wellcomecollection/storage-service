data "aws_ssm_parameter" "admin_cidr_ingress" {
  name = "/storage/config/prod/admin_cidr_ingress"
}

data "aws_ssm_parameter" "bagger_mets_bucket_name" {
  name = "/storage/config/prod/bagger_mets_bucket_name"
}

data "aws_ssm_parameter" "bagger_read_mets_from_fileshare" {
  name = "/storage/config/prod/bagger_read_mets_from_fileshare"
}

data "aws_ssm_parameter" "bagger_working_directory" {
  name = "/storage/config/prod/bagger_working_directory"
}

data "aws_ssm_parameter" "bagger_current_preservation_bucket" {
  name = "/storage/config/prod/bagger_current_preservation_bucket"
}

data "aws_ssm_parameter" "bagger_dlcs_entry" {
  name = "/storage/config/prod/bagger_dlcs_entry"
}

data "aws_ssm_parameter" "bagger_dlcs_customer_id" {
  name = "/storage/config/prod/bagger_dlcs_customer_id"
}

data "aws_ssm_parameter" "bagger_dlcs_space" {
  name = "/storage/config/prod/bagger_dlcs_space"
}

data "aws_ssm_parameter" "bagger_dds_asset_prefix" {
  name = "/storage/config/prod/bagger_dds_asset_prefix"
}

data "aws_ssm_parameter" "bagger_dlcs_source_bucket" {
  name = "/storage/config/prod/bagger_dlcs_source_bucket"
}

data "aws_ssm_parameter" "archive_oauth_details_enc" {
  name = "/storage/config/prod/archive_oauth_details_enc"
}

locals {
  admin_cidr_ingress = "${data.aws_ssm_parameter.admin_cidr_ingress.value}"

  bagger_mets_bucket_name            = "${data.aws_ssm_parameter.bagger_mets_bucket_name.value}"
  bagger_read_mets_from_fileshare    = "${data.aws_ssm_parameter.bagger_read_mets_from_fileshare.value == "true" ? true : false}"
  bagger_working_directory           = "${data.aws_ssm_parameter.bagger_working_directory.value}"
  bagger_current_preservation_bucket = "${data.aws_ssm_parameter.bagger_current_preservation_bucket.value}"

  bagger_dlcs_entry         = "${data.aws_ssm_parameter.bagger_dlcs_entry.value}"
  bagger_dlcs_customer_id   = "${data.aws_ssm_parameter.bagger_dlcs_customer_id.value}"
  bagger_dlcs_space         = "${data.aws_ssm_parameter.bagger_dlcs_space.value}"
  bagger_dlcs_source_bucket = "${data.aws_ssm_parameter.bagger_dlcs_source_bucket.value}"

  bagger_dds_asset_prefix = "${data.aws_ssm_parameter.bagger_dds_asset_prefix.value}"

  archive_oauth_details_enc = "${data.aws_ssm_parameter.archive_oauth_details_enc.value}"

  key_name = "wellcomedigitalstorage"
}

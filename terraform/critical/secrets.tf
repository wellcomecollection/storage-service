module "bagger_dlcs_api_key" {
  source = "git::https://github.com/wellcometrust/terraform.git//kms/modules/secret?ref=32a35244da54d3dce5991bfe7ca60fff1f952163"

  name       = "storage/${var.environment_name}/bagger/dlcs/api/key"
  kms_key_id = "${module.kms_key.encryption_key_id}"
}

module "bagger_dlcs_api_secret" {
  source = "git::https://github.com/wellcometrust/terraform.git//kms/modules/secret?ref=32a35244da54d3dce5991bfe7ca60fff1f952163"

  name       = "storage/${var.environment_name}/bagger/dlcs/api/secret"
  kms_key_id = "${module.kms_key.encryption_key_id}"
}

module "bagger_dds_api_key" {
  source = "git::https://github.com/wellcometrust/terraform.git//kms/modules/secret?ref=32a35244da54d3dce5991bfe7ca60fff1f952163"

  name       = "storage/${var.environment_name}/bagger/dds/api/key"
  kms_key_id = "${module.kms_key.encryption_key_id}"
}

module "bagger_dds_api_secret" {
  source = "git::https://github.com/wellcometrust/terraform.git//kms/modules/secret?ref=32a35244da54d3dce5991bfe7ca60fff1f952163"

  name       = "storage/${var.environment_name}/bagger/dds/api/secret"
  kms_key_id = "${module.kms_key.encryption_key_id}"
}

module "bagger_aws_access_key_id" {
  source = "git::https://github.com/wellcometrust/terraform.git//kms/modules/secret?ref=32a35244da54d3dce5991bfe7ca60fff1f952163"

  name       = "storage/${var.environment_name}/bagger/aws/key/id"
  kms_key_id = "${module.kms_key.encryption_key_id}"
}

module "bagger_aws_secret_access_key" {
  source = "git::https://github.com/wellcometrust/terraform.git//kms/modules/secret?ref=32a35244da54d3dce5991bfe7ca60fff1f952163"

  name       = "storage/${var.environment_name}/bagger/aws/key/secret"
  kms_key_id = "${module.kms_key.encryption_key_id}"
}

module "archive_oauth_details_enc" {
  source = "git::https://github.com/wellcometrust/terraform.git//kms/modules/secret?ref=32a35244da54d3dce5991bfe7ca60fff1f952163"

  name       = "storage/${var.environment_name}/bagger/oauth/token"
  kms_key_id = "${module.kms_key.encryption_key_id}"
}

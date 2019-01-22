module "kms_key" {
  source     = "git::https://github.com/wellcometrust/terraform.git//kms/modules/key?ref=32a35244da54d3dce5991bfe7ca60fff1f952163"
  account_id = "${var.account_id}"
}

module "kms_key" {
  source     = "git::https://github.com/wellcometrust/terraform.git//kms/modules/key?ref=v19.8.0"
  account_id = "${var.account_id}"
}

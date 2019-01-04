module "kms_key" {
  source     = "git::https://github.com/wellcometrust/terraform.git//kms/modules/key?ref=v18.2.0"
  account_id = "${var.account_id}"
}

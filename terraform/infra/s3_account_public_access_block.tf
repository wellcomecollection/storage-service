resource "aws_s3_account_public_access_block" "storage_account" {
  block_public_acls   = true
  block_public_policy = true
  ignore_public_acls  = true
}

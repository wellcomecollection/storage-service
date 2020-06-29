module "vhs_manifests" {
  source = "git::github.com/wellcomecollection/terraform-aws-vhs.git//hash-range-store?ref=v3.3.1"
  name   = "${var.namespace}-manifests"

  tags = var.tags

  # These prefixes exist for compatibility with older versions of the VHS
  # Terraform module.  Renaming S3 buckets or DynamoDB tables is hard, so
  # we preserve the existing names rather than change them.
  bucket_name_prefix = "wellcomecollection-vhs-"
  table_name_prefix  = "vhs-"

  table_name = var.table_name
}

module "vhs_manifests" {
  source = "git::github.com/wellcomecollection/terraform-aws-vhs.git//multi-version-store?ref=v4.2.0"
  name   = "${var.namespace}-manifests"

  bucket_name = var.vhs_bucket_name
  table_name  = var.vhs_table_name
}

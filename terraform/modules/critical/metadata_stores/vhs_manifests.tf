module "vhs_manifests" {
  source = "./multi-version-store"
  name   = "${var.namespace}-manifests"

  bucket_name = var.vhs_bucket_name
  table_name  = var.vhs_table_name
}

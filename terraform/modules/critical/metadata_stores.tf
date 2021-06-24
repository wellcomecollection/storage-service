module "metadata_stores" {
  source = "./metadata_stores"

  namespace = var.namespace

  vhs_bucket_name = "wellcomecollection-vhs-${var.namespace}-manifests"
  vhs_table_name  = var.table_name
}

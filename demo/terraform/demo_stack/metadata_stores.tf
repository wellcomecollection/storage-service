module "metadata_stores" {
  source = "../../../terraform/modules/critical/metadata_stores"

  namespace = var.namespace

  vhs_bucket_name = "${var.namespace}-manifests"
  vhs_table_name  = "${var.namespace}-manifests"
}

module "metadata_stores" {
  source = "github.com/wellcomecollection/storage-service.git//terraform/modules/critical/metadata_stores?ref=a0979ee"

  namespace = var.namespace

  vhs_bucket_name = "${var.namespace}-manifests"
  vhs_table_name  = "${var.namespace}-manifests"
}

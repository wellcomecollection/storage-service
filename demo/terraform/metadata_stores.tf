module "metadata_stores" {
  source = "github.com/wellcomecollection/storage-service.git//terraform/modules/critical/metadata_stores?ref=b24ea38"

  namespace = local.namespace

  vhs_bucket_name = "${local.namespace}-manifests"
  vhs_table_name  = "${local.namespace}-manifests"
}

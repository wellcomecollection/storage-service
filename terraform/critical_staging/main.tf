module "critical" {
  source = "../modules/critical"

  namespace = "${local.namespace}-staging"

  replica_primary_read_principals = [
    for account_id in values(local.account_ids):
    "arn:aws:iam::${account_id}:root"
  ]

  azure_storage_account_name = "wecostoragestage"
  azure_resource_group_name  = "rg-wcollarchive-stage"

  inventory_bucket = "wellcomecollection-storage-infra"

  tags = local.default_tags

  table_name = "vhs-storage-staging-manifests-2020-07-24"

  # The staging service shouldn't be the only copy of any important data, so
  # we don't need S3 versioning.
  enable_s3_versioning = false
}
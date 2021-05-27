module "critical" {
  source = "../modules/critical"

  namespace = local.namespace

  replica_primary_read_principals = [
    for account_id in values(local.account_ids) :
    "arn:aws:iam::${account_id}:root"
  ]

  azure_storage_account_name = "wecostorageprod"
  azure_resource_group_name  = "rg-wcollarchive-prod"

  inventory_bucket = "wellcomecollection-storage-infra"

  table_name = "vhs-storage-manifests-2020-07-24"

  # This gives us another layer of protection for the S3 buckets.
  #
  # In theory, every object is written once and exactly once:
  #
  #   - The AWS roles used by developers have an explicit Deny for
  #     PutObject or DeleteObject permissions, so we can't overwrite or
  #     remove objects from the buckets.
  #   - The replicators check for the existence of another object before
  #     writing anything to the bucket, and we have locking in place to
  #     ensure only one replicator is writing to a given prefix at a time.
  #
  # This means that there shouldn't be any extra cost to versioning, because
  # we're only ever storing a single version of each object.
  #
  # Enabling versioning gets us an extra level of protection, so if an object
  # is accidentally deleted, we can recover it.  We shouldn't rely on it, but
  # there's essentially no downside to having it enabled here.
  enable_s3_versioning = true
}

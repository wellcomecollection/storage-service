# These policies prevent users with the "admin" and "developer" roles from
# interacting with the databases in the production storage service.
#
# In particular, they should prevent us from:
#
#   * writing directly to S3 or DynamoDB
#   * deleting anything from either of those data stores
#
# We should never be manually interacting with these databases.
#
# These protections are only applied to the production databases; we can still
# do all these things in the staging service.

data "aws_iam_role" "admin" {
  name = "storage-admin"
}

data "aws_iam_role" "developer" {
  name = "storage-developer"
}

locals {
  storage_roles = [
    data.aws_iam_role.admin.name,
    data.aws_iam_role.developer.name,
  ]
}

data "aws_iam_policy_document" "prevent_writes_to_prod" {
  statement {
    actions = [
      "dynamodb:Delete*",
      "dynamodb:Put*",
      "dynamodb:Update*",
    ]

    effect = "Deny"

    resources = [
      module.critical.ingests_table_arn,
      "${module.critical.ingests_table_arn}/*",
      module.critical.vhs_manifests_table_arn,
      "${module.critical.vhs_manifests_table_arn}/*",
      module.critical.replicas_table_arn,
      "${module.critical.replicas_table_arn}/*",
      module.critical.versions_table_arn,
      "${module.critical.versions_table_arn}/*",
    ]
  }

  statement {
    actions = [
      "s3:Delete*",

      # Although these two statements could be combined into "s3:Put*",
      # we leave them separate so we can turn them on/off separately.
      #
      # In particular, when we want to change bucket policies or permissions,
      # we can give ourselves PutBucket permissions without unlocking the
      # ability to overwrite objects in the bucket.
      "s3:PutObject*",
      "s3:PutBucket*",
    ]

    effect = "Deny"

    resources = [
      module.critical.replica_primary_bucket_arn,
      "${module.critical.replica_primary_bucket_arn}/*",
      module.critical.replica_glacier_bucket_arn,
      "${module.critical.replica_glacier_bucket_arn}/*",
      module.critical.vhs_manifests_bucket_arn,
      "${module.critical.vhs_manifests_bucket_arn}/*",
    ]
  }
}

resource "aws_iam_role_policy" "prevent_deletions" {
  count = length(local.storage_roles)

  role   = element(local.storage_roles, count.index)
  policy = data.aws_iam_policy_document.prevent_writes_to_prod.json
}


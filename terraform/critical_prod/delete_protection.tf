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
    "${data.aws_iam_role.admin.name}",
    "${data.aws_iam_role.developer.name}",
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
      "${module.critical.ingests_table_arn}",
      "${module.critical.ingests_table_arn}/*",

      "${module.critical.manifests_table_arn}",
      "${module.critical.manifests_table_arn}/*",

      "${module.critical.replicas_table_arn}",
      "${module.critical.replicas_table_arn}/*",

      "${module.critical.versions_table_arn}",
      "${module.critical.versions_table_arn}/*",
    ]
  }

  statement {
    actions = [
      "s3:Delete*",
      "s3:Put*",
    ]

    effect = "Deny"

    resources = [
      "${module.critical.access_bucket_arn}",
      "${module.critical.access_bucket_arn}/*",

      "${module.critical.archive_bucket_arn}",
      "${module.critical.archive_bucket_arn}/*",

      "${module.critical.manifests_bucket_arn}",
      "${module.critical.manifests_bucket_arn}/*",
    ]
  }
}

resource "aws_iam_role_policy" "prevent_deletions" {
  count = "${length(local.storage_roles)}"

  role   = "${element(local.storage_roles, count.index)}"
  policy = "${data.aws_iam_policy_document.prevent_writes_to_prod.json}"
}

resource "aws_iam_role_policy" "bag_verifier_metrics" {
  role   = module.bag_verifier.task_role_name
  policy = var.cloudwatch_metrics_policy_json
}

# The bag verifier needs to be able to read objects from the bucket where
# the replica is written to.

data "aws_iam_policy_document" "bucket_read" {
  statement {
    actions = [
      "s3:ListBucket",
    ]

    resources = [
      "arn:aws:s3:::${var.destination_namespace}",
    ]
  }

  statement {
    actions = [
      "s3:GetObject*",
    ]

    resources = [
      "arn:aws:s3:::${var.destination_namespace}/*",
    ]
  }

  statement {
    actions = [
      "s3:PutObjectTagging",
    ]

    resources = [
      "arn:aws:s3:::${var.destination_namespace}/*",
    ]
  }
}

resource "aws_iam_role_policy" "bag_verifier_read" {
  role   = module.bag_verifier.task_role_name
  policy = data.aws_iam_policy_document.bucket_read.json
}

# The fetch.txt entry may refer to locations in the primary bucket, so we need
# to give verifier permissions on that bucket as well.

data "aws_iam_policy_document" "primary_bucket_read" {
  statement {
    actions = [
      "s3:GetObject*",
    ]

    resources = [
      "arn:aws:s3:::${var.primary_bucket_name}",
      "arn:aws:s3:::${var.primary_bucket_name}/*",
    ]
  }

  statement {
    actions = [
      "s3:PutObjectTagging",
    ]

    resources = [
      "arn:aws:s3:::${var.primary_bucket_name}/*",
    ]
  }
}

resource "aws_iam_role_policy" "bag_verifier_primary_read" {
  role   = module.bag_verifier.task_role_name
  policy = data.aws_iam_policy_document.primary_bucket_read.json
}

# The post-replicator verifiers compare the tagmanifest-sha256.txt in the
# original bag and the replicated bag, so they need to be able to read from
# the bucket where the bags are unpacked.

data "aws_iam_policy_document" "bag_verifier_unpacked_bucket_read" {
  statement {
    actions = [
      "s3:GetObject*",
    ]

    resources = [
      "arn:aws:s3:::${var.unpacker_bucket_name}/*"
    ]
  }
}

resource "aws_iam_role_policy" "bag_verifier_unpacked_bucket_read" {
  role   = module.bag_verifier.task_role_name
  policy = data.aws_iam_policy_document.bag_verifier_unpacked_bucket_read.json
}

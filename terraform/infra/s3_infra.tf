resource "aws_s3_bucket" "infra" {
  bucket = "wellcomecollection-storage-infra"
  acl    = "private"

  lifecycle {
    prevent_destroy = true
  }

  versioning {
    enabled = true
  }
}

data "aws_iam_policy_document" "infra_bucket_policy" {
  # We use S3 Inventory to write an inventory for our storage buckets to
  # the infra bucket, so we need to allow S3 to write to this bucket.
  #
  # https://docs.aws.amazon.com/AmazonS3/latest/dev/example-bucket-policies.html#example-bucket-policies-use-case-9
  statement {
    actions = [
      "s3:PutObject",
    ]

    resources = [
      "${aws_s3_bucket.infra.arn}/s3_inventory/*",
    ]

    principals {
      type        = "Service"
      identifiers = ["s3.amazonaws.com"]
    }

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"

      values = [
        "arn:aws:s3:::wellcomecollection-storage",
        "arn:aws:s3:::wellcomecollection-storage-replica-ireland",
        "arn:aws:s3:::wellcomecollection-storage-staging",
        "arn:aws:s3:::wellcomecollection-storage-staging-replica-ireland",
      ]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = ["975596993436"]
    }

    condition {
      test     = "StringEquals"
      variable = "s3:x-amz-acl"
      values   = ["bucket-owner-full-control"]
    }
  }
}

resource "aws_s3_bucket_policy" "infra" {
  bucket = aws_s3_bucket.infra.id
  policy = data.aws_iam_policy_document.infra_bucket_policy.json
}

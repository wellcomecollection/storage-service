resource "aws_s3_bucket" "replica_glacier" {
  bucket = "wellcomecollection-${var.namespace}-replica-ireland"
  acl    = "private"

  lifecycle_rule {
    enabled = true

    transition {
      days          = 90
      storage_class = "DEEP_ARCHIVE"
    }
  }
}

resource "aws_s3_bucket_policy" "replica_glacier_read" {
  count = length(var.replica_glacier_read_principals) == 0 ? 0 : 1

  bucket = aws_s3_bucket.replica_glacier.id
  policy = data.aws_iam_policy_document.replica_glacier_readonly.json
}

data "aws_iam_policy_document" "replica_glacier_readonly" {
  statement {
    actions = [
      "s3:List*",
      "s3:Get*",
    ]

    resources = [
      "${aws_s3_bucket.replica_glacier.arn}",
      "${aws_s3_bucket.replica_glacier.arn}/*",
    ]

    principals {
      type = "AWS"

      identifiers = var.replica_glacier_read_principals
    }
  }
}

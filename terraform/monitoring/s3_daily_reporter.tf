# Create a bucket with static website hosting to host the storage service
# daily reports.  This has to live in the platform account because we block
# creating buckets with public access in the storage account.

resource "aws_s3_bucket" "daily_reporter" {
  bucket = "wellcomecollection-storage-daily-reporting"

  provider = aws.platform

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_acl" "daily_reporter" {
  bucket = aws_s3_bucket.daily_reporter.id
  acl = "public-read"

  provider = aws.platform
}

resource "aws_s3_bucket_website_configuration" "daily_reporter" {
  bucket = aws_s3_bucket.daily_reporter.id

  index_document {
    suffix = "index.html"
  }

  provider = aws.platform
}

resource "aws_s3_bucket_lifecycle_configuration" "daily_reporter" {
  bucket = aws_s3_bucket.daily_reporter.id

  rule {
    id     = "expire_old_reports"
    status = "Enabled"

    expiration {
      days = 90
    }
  }

  provider = aws.platform
}

resource "aws_s3_bucket_policy" "daily_reporter_storage_access" {
  bucket = aws_s3_bucket.daily_reporter.id
  policy = data.aws_iam_policy_document.daily_reporter_storage_access.json

  provider = aws.platform
}

data "aws_caller_identity" "current" {}

data "aws_iam_policy_document" "daily_reporter_storage_access" {
  statement {
    actions = [
      "s3:PutObject*",
      "s3:GetObject*",
    ]

    resources = [
      aws_s3_bucket.daily_reporter.arn,
      "${aws_s3_bucket.daily_reporter.arn}/*",
    ]

    principals {
      type = "AWS"

      identifiers = [
        data.aws_caller_identity.current.account_id,
      ]
    }
  }
}

resource "aws_s3_bucket" "replica_primary" {
  bucket = "wellcomecollection-${var.namespace}"
  acl    = "private"

  versioning {
    enabled = var.enable_s3_versioning
  }

  lifecycle_rule {
    id      = "transition_objects_to_standard_ia"
    enabled = true

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }

  lifecycle_rule {
    id      = "move_mxf_objects_to_glacier"
    enabled = true

    tags = {
      "Content-Type" = "application/mxf"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }

  tags = var.tags
}

resource "aws_s3_bucket_policy" "replica_primary_read" {
  bucket = aws_s3_bucket.replica_primary.id
  policy = data.aws_iam_policy_document.replica_primary_read.json
}

data "aws_iam_policy_document" "replica_primary_read" {
  statement {
    actions = [
      "s3:List*",
      "s3:Get*",
    ]

    resources = [
      "${aws_s3_bucket.replica_primary.arn}",
      "${aws_s3_bucket.replica_primary.arn}/*",
    ]

    principals {
      type = "AWS"

      identifiers = sort(var.replica_primary_read_principals)
    }
  }
}

resource "aws_s3_bucket_inventory" "replica_primary" {
  bucket = aws_s3_bucket.replica_primary.id
  name   = "ReplicaPrimaryWeekly"

  included_object_versions = "All"

  schedule {
    frequency = "Weekly"
  }

  optional_fields = [
    "Size",
    "LastModifiedDate",
    "StorageClass",
    "ETag",
  ]

  destination {
    bucket {
      format     = "CSV"
      bucket_arn = "arn:aws:s3:::${var.inventory_bucket}"
      prefix     = "s3_inventory"
    }
  }
}

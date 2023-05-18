resource "aws_s3_bucket" "replica_primary" {
  bucket = "wellcomecollection-${var.namespace}"
}

resource "aws_s3_bucket_acl" "replica_primary" {
  bucket = aws_s3_bucket.replica_primary.id
  acl    = "private"
}

resource "aws_s3_bucket_versioning" "replica_primary" {
  bucket = aws_s3_bucket.replica_primary.id

  versioning_configuration {
    status = var.enable_s3_versioning ? "Enabled" : "Disabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "replica_primary" {
  bucket = aws_s3_bucket.replica_primary.id

  rule {
    id      = "transition_objects_to_standard_ia"
    status = "Enabled"

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }

  rule {
    id      = "move_mxf_objects_to_glacier"
    status = "Enabled"

    filter {
      and {
        prefix = "digitised/"

        tags = {
          "Content-Type" = "application/mxf"
        }
      }
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }

  # In general, these permanent storage buckets should follow
  # Write-Once, Read-Many (WORM).
  #
  # It's extremely unusual for us to delete objects.  Enabling versioning gives
  # us a safety net against accidental deletions -- if we delete something, we can
  # recover it -- but we do want deleted objects to disappear eventually,
  # e.g. for data protection.
  rule {
    id      = "expire_noncurrent_versions"
    status = var.enable_s3_versioning ? "Enabled" : "Disabled"

    noncurrent_version_transition {
      noncurrent_days          = 30
      storage_class = "STANDARD_IA"
    }

    noncurrent_version_transition {
      noncurrent_days          = 60
      storage_class = "GLACIER"
    }

    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }

  # See comment in TagRules.scala -- this is about moving high-resolution
  # TIFFs in our manuscripts workflow to Glacier.
  rule {
    id      = "move_digitised_tif_to_glacier"
    status = "Enabled"

    filter {
      and {
        prefix = "digitised/"

        tags = {
          "Content-Type" = "image/tiff"
        }
      }
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
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
      aws_s3_bucket.replica_primary.arn,
      "${aws_s3_bucket.replica_primary.arn}/*",
    ]

    principals {
      type = "AWS"

      identifiers = sort(var.replica_primary_read_principals)
    }
  }

  # Created so that Digirati/DDS can read both the prod and the staging buckets.

  statement {
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]

    resources = [
      aws_s3_bucket.replica_primary.arn,
      "${aws_s3_bucket.replica_primary.arn}/*",
    ]

    condition {
      test     = "StringLike"
      variable = "aws:userId"

      values = [
        "AROAZQI22QHW3LZ4TYY54:*",

        # For the auxiliary ingest engine
        # See https://wellcome.slack.com/archives/CBT40CMKQ/p1569923258424800
        "AROAZQI22QHWUG2I4CBRN:*",

        # For the Tizer engine
        # See https://wellcome.slack.com/archives/CBT40CMKQ/p1570188255112200
        #     https://wellcome.slack.com/archives/CBT40CMKQ/p1574954471260700
        "AROAZQI22QHWYAPBYZG6U:*",

        # For video ingests
        # See https://wellcome.slack.com/archives/CBT40CMKQ/p1571310993345000
        "AROAZQI22QHWV2KHZZHCT:*",

        # Beta version of the DLCS orchestrator.
        # See https://wellcome.slack.com/archives/CBT40CMKQ/p1573742247457800
        "AROAZQI22QHWTHLN4QHJU:*",
      ]
    }

    principals {
      type = "AWS"

      identifiers = [
        "*",
      ]
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

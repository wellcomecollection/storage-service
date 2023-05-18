resource "aws_s3_bucket" "replica_glacier" {
  bucket = "wellcomecollection-${var.namespace}-replica-ireland"
}

resource "aws_s3_bucket_acl" "replica_glacier" {
  bucket = aws_s3_bucket.replica_glacier.id
  acl    = "private"
}

resource "aws_s3_bucket_versioning" "replica_glacier" {
  bucket = aws_s3_bucket.replica_glacier.id

  versioning_configuration {
    status = var.enable_s3_versioning ? "Enabled" : "Disabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "replica_glacier" {
  bucket = aws_s3_bucket.replica_glacier.id

  rule {
    id     = "transition_objects_to_deep_archive"
    status = "Enabled"

    transition {
      days          = 90
      storage_class = "DEEP_ARCHIVE"
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
    id     = "expire_noncurrent_versions"
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
}

resource "aws_s3_bucket_inventory" "replica_glacier" {
  bucket = aws_s3_bucket.replica_glacier.id
  name   = "ReplicaGlacierWeekly"

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

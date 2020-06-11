resource "aws_s3_bucket" "replica_glacier" {
  bucket = "wellcomecollection-${var.namespace}-replica-ireland"
  acl    = "private"

  versioning {
    enabled = var.enable_s3_versioning
  }

  lifecycle_rule {
    id      = "transition_objects_to_deep_archive"
    enabled = true

    transition {
      days          = 90
      storage_class = "DEEP_ARCHIVE"
    }
  }

  tags = var.tags
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

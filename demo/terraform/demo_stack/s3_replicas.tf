# These extra buckets are the permanent copy of our preservation files.

resource "aws_s3_bucket" "replica_primary" {
  bucket = "${var.namespace}-replica-primary"
  acl    = "private"

  # Note: we enable versioning in our real buckets, but I've disabled
  # it here because this is just a prototype.  This system shouldn't be
  # the only copy of any real data.
  versioning {
    enabled = false
  }

  lifecycle_rule {
    id      = "transition_objects_to_standard_ia"
    enabled = true

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }
}

# TODO: Create an S3 inventory of the primary replica bucket.

resource "aws_s3_bucket" "replica_glacier" {
  bucket = "${var.namespace}-replica-glacier"
  acl    = "private"

  # Note: see above.
  versioning {
    enabled = false
  }

  lifecycle_rule {
    id      = "transition_objects_to_deep_archive"
    enabled = true

    transition {
      days          = 90
      storage_class = "DEEP_ARCHIVE"
    }
  }
}

# TODO: Create an S3 inventory of the Glacier replica bucket.

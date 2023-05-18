resource "aws_s3_bucket" "bucket" {
  bucket = local.bucket_name

  tags = var.tags
}

resource "aws_s3_bucket_lifecycle_configuration" "bucket" {
  bucket = local.bucket_name

  rule {
    id      = "objects_to_standard_ia"
    status = var.cycle_objects_to_standard_ia ? "Enabled" : "Disabled"

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }
}

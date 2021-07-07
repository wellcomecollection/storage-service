resource "aws_s3_bucket" "uploads" {
  bucket = "${var.namespace}-uploads"
  acl    = "private"
}

resource "aws_s3_bucket_object" "example_bag" {
  bucket = aws_s3_bucket.uploads.bucket
  key    = "example_bag.tar.gz"
  source = "example_bag.tar.gz"
}

variable "infra_bucket" {
  type = string
}

variable "test_bag_prefix" {
  type = string
}

variable "filename" {
  type = string
}

variable "tags" {
  type = map(string)
}

resource "aws_s3_bucket_object" "bag" {
  bucket = var.infra_bucket
  key    = "${var.test_bag_prefix}${var.filename}"
  source = "${path.module}/../../../monitoring/test_bags/${var.filename}"

  etag = filemd5("${path.module}/../../../monitoring/test_bags/${var.filename}")

  tags = var.tags
}

output "bucket" {
  value = var.infra_bucket
}

output "key" {
  value = aws_s3_bucket_object.bag.id
}

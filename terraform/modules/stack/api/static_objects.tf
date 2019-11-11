resource "aws_s3_bucket_object" "context" {
  bucket  = var.static_content_bucket_name
  key     = "static/context.json"
  content = file("${path.module}/context.json")
  etag    = filemd5("${path.module}/context.json")
}


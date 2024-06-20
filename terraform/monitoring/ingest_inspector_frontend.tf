resource "aws_s3_bucket" "ingest_inspector_frontend" {
  bucket   = "wellcomecollection-ingest-inspector-static-frontend"
  provider = aws.platform
}


resource "aws_s3_bucket_public_access_block" "ingest_inspector_frontend" {
  bucket   = aws_s3_bucket.ingest_inspector_frontend.id
  provider = aws.platform

  block_public_acls       = false
  block_public_policy     = false
  ignore_public_acls      = false
  restrict_public_buckets = false
}

resource "aws_s3_bucket_website_configuration" "ingest_inspector_frontend" {
  bucket   = aws_s3_bucket.ingest_inspector_frontend.id
  provider = aws.platform

  index_document {
    suffix = "index.html"
  }

  error_document {
    key = "error.html"
  }
}

resource "aws_s3_bucket_ownership_controls" "ingest_inspector_frontend" {
  bucket   = aws_s3_bucket.ingest_inspector_frontend.id
  provider = aws.platform

  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

resource "aws_s3_bucket_acl" "site" {
  bucket   = aws_s3_bucket.ingest_inspector_frontend.id
  provider = aws.platform

  acl = "public-read"
  depends_on = [
    aws_s3_bucket_ownership_controls.ingest_inspector_frontend,
    aws_s3_bucket_public_access_block.ingest_inspector_frontend
  ]
}

resource "aws_s3_bucket_policy" "ingest_inspector_frontend" {
  bucket   = aws_s3_bucket.ingest_inspector_frontend.id
  provider = aws.platform

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "PublicReadGetObject"
        Effect    = "Allow"
        Principal = "*"
        Action    = "s3:GetObject"
        Resource = [
          aws_s3_bucket.ingest_inspector_frontend.arn,
          "${aws_s3_bucket.ingest_inspector_frontend.arn}/*",
        ]
      },
    ]
  })

  depends_on = [
    aws_s3_bucket_public_access_block.ingest_inspector_frontend
  ]
}

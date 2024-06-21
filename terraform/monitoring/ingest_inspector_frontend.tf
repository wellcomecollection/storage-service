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

locals {
  s3_origin_id   = "${aws_s3_bucket.ingest_inspector_frontend.bucket}-origin"
  s3_domain_name = "${aws_s3_bucket.ingest_inspector_frontend.bucket}.s3-website-eu-west-1.amazonaws.com"
}

resource "aws_cloudfront_distribution" "ingest_inspector_cloudfront_distribution" {
  enabled = true
  provider = aws.platform

  origin {
    origin_id                = local.s3_origin_id
    domain_name              = local.s3_domain_name
    custom_origin_config {
      http_port              = 80
      https_port             = 443
      origin_protocol_policy = "http-only"
      origin_ssl_protocols   = ["TLSv1"]
    }
  }

  default_cache_behavior {

    target_origin_id = local.s3_origin_id
    allowed_methods  = ["GET", "HEAD"]
    cached_methods   = ["GET", "HEAD"]

    forwarded_values {
      query_string = true

      cookies {
        forward = "all"
      }
    }

    viewer_protocol_policy = "redirect-to-https"
    min_ttl                = 0
    default_ttl            = 0
    max_ttl                = 0
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }

  price_class = "PriceClass_100"
}

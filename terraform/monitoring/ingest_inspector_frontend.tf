resource "aws_s3_bucket" "ingest_inspector_frontend" {
  bucket   = "wellcomecollection-ingest-inspector-frontend"
}

locals {
  s3_origin_id   = "origin-${aws_s3_bucket.ingest_inspector_frontend.bucket}"
}

# Create a CloudFront distribution to serve the contents of the bucket via HTTPS
resource "aws_cloudfront_distribution" "ingest_inspector_cloudfront_distribution" {
  enabled             = true
  default_root_object = "index.html"

  origin {
    origin_id                = local.s3_origin_id
    domain_name              = aws_s3_bucket.ingest_inspector_frontend.bucket_regional_domain_name
    origin_access_control_id = aws_cloudfront_origin_access_control.ingest_inspector_oac.id
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

# Create an OAC to allow CloudFront to securely access the static website on S3 while keeping the bucket private
resource "aws_cloudfront_origin_access_control" "ingest_inspector_oac" {
  name                              = "allow-cloudfront-s3-ingest-inspector-access"
  description                       = "Allows CloudFront to serve the contents of the private ${aws_s3_bucket.ingest_inspector_frontend.bucket} bucket"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

resource "aws_s3_bucket_policy" "allow_cloudfront_get_object" {
  bucket = aws_s3_bucket.ingest_inspector_frontend.id
  policy = data.aws_iam_policy_document.cloudfront_oac_access.json
}

data "aws_iam_policy_document" "cloudfront_oac_access" {
  statement {
    principals {
      type        = "Service"
      identifiers = ["cloudfront.amazonaws.com"]
    }

    actions = [
      "s3:GetObject"
    ]

    resources = [
      aws_s3_bucket.ingest_inspector_frontend.arn,
      "${aws_s3_bucket.ingest_inspector_frontend.arn}/*"
    ]

    condition {
      test     = "StringEquals"
      variable = "AWS:SourceArn"
      values   = [aws_cloudfront_distribution.ingest_inspector_cloudfront_distribution.arn]
    }
  }
}

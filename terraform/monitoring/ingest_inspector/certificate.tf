module "ingest_inspector_certificate" {
  source = "github.com/wellcomecollection/terraform-aws-acm-certificate?ref=v1.0.0"

  domain_name = var.domain_name
  zone_id = data.aws_route53_zone.weco_zone.id

  providers = {
    # The ACM certificate must be created in the us-east-1 region to work with CloudFront
    aws     = aws.us-east-1
    aws.dns = aws.dns
  }
}

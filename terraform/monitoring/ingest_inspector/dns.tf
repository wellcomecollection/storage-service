# Add an A record to the wellcomecollection.org hosted zone 
resource "aws_route53_record" "cdn" {
  provider = aws.dns
  zone_id  = data.aws_route53_zone.weco_zone.id
  name     = var.domain_name
  type     = "A"

  alias {
    name                   = aws_cloudfront_distribution.ingest_inspector_cloudfront_distribution.domain_name
    zone_id                = aws_cloudfront_distribution.ingest_inspector_cloudfront_distribution.hosted_zone_id
    evaluate_target_health = false
  }
}

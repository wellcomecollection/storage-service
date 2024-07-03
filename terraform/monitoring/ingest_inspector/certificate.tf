# The ACM certificate must be created in the us-east-1 region to work with CloudFront
resource "aws_acm_certificate" "ingest_inspector_certificate" {
  provider = aws.us-east-1
  domain_name       = var.domain_name
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_route53_record" "cert_validation" {
  provider = aws.dns

  for_each = {
    for dvo in aws_acm_certificate.ingest_inspector_certificate.domain_validation_options : dvo.domain_name => {
      name   = dvo.resource_record_name
      record = dvo.resource_record_value
      type   = dvo.resource_record_type
    }
  }

  name    = each.value.name
  type    = each.value.type
  records = [each.value.record]

  zone_id = data.aws_route53_zone.weco_zone.id

  ttl = 60
}

resource "aws_acm_certificate_validation" "validation" {
  provider = aws.us-east-1
  certificate_arn = aws_acm_certificate.ingest_inspector_certificate.arn
  validation_record_fqdns = [for record in aws_route53_record.cert_validation : record.fqdn]
}

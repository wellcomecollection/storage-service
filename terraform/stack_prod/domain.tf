resource "aws_api_gateway_base_path_mapping" "mapping_staging" {
  api_id      = module.stack_prod.api_gateway_id
  domain_name = local.domain_name
  base_path   = "storage"
}

module "domain" {
  source = "../modules/stack/api/domain"

  domain_name      = local.domain_name
  cert_domain_name = local.cert_domain_name
}

data "aws_route53_zone" "zone" {
  provider = aws.dns

  name = "wellcomecollection.org."
}

resource "aws_route53_record" "origin" {
  zone_id = data.aws_route53_zone.zone.id
  name    = local.domain_name
  type    = "CNAME"
  records = [module.domain.regional_domain_name]
  ttl     = "300"

  provider = aws.dns
}

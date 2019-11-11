data "aws_route53_zone" "internal" {
  name         = "storage.internal."
  private_zone = true
}

resource "aws_vpc_endpoint" "pl-winslow" {
  vpc_id            = local.vpc_id
  service_name      = local.service-pl-winslow
  vpc_endpoint_type = "Interface"

  security_group_ids = [
    module.stack_staging.interservice_sg_id,
  ]

  subnet_ids = local.subnets_ids

  private_dns_enabled = false
}

resource "aws_route53_record" "pl-winslow" {
  zone_id = data.aws_route53_zone.internal.zone_id
  name    = "pl-winslow.${data.aws_route53_zone.internal.name}"
  type    = "CNAME"
  ttl     = "300"

  records = [
    aws_vpc_endpoint.pl-winslow.dns_entry[0]["dns_name"],
  ]
}


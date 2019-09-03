resource "aws_route53_zone" "internal" {
  name = "storage.internal."

  vpc {
    vpc_id = "${local.vpc_id}"
  }
}

# pl-winslow


# wt-winnipeg

resource "aws_vpc_endpoint" "wt-winnipeg" {
  vpc_id            = "${local.vpc_id}"
  service_name      = "${local.service-wt-winnipeg}"
  vpc_endpoint_type = "Interface"

  security_group_ids = [
    "${module.stack_prod.interservice_sg_id}",
  ]

  subnet_ids = [
    "${local.subnets_ids}",
  ]

  private_dns_enabled = false
}

resource "aws_route53_record" "wt-winnipeg" {
  zone_id = "${aws_route53_zone.internal.zone_id}"
  name    = "wt-winnipeg.${aws_route53_zone.internal.name}"
  type    = "CNAME"
  ttl     = "300"

  records = [
    "${lookup(aws_vpc_endpoint.wt-winnipeg.dns_entry[0], "dns_name")}",
  ]
}

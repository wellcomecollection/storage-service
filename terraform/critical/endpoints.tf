resource "aws_route53_zone" "internal" {
  name = "storage.internal."

  vpc {
    vpc_id = "${var.vpc_id}"
  }
}

# pl-winslow

resource "aws_vpc_endpoint" "pl-winslow" {
  vpc_id            = "${var.vpc_id}"
  service_name      = "${var.service-pl-winslow}"
  vpc_endpoint_type = "Interface"

  security_group_ids = [
    "${aws_security_group.interservice.id}",
  ]

  subnet_ids = [
    "${var.subnets_ids}",
  ]

  private_dns_enabled = false
}

resource "aws_route53_record" "pl-winslow" {
  zone_id = "${aws_route53_zone.internal.zone_id}"
  name    = "pl-winslow.${aws_route53_zone.internal.name}"
  type    = "CNAME"
  ttl     = "300"

  records = [
    "${lookup(aws_vpc_endpoint.pl-winslow.dns_entry[0], "dns_name")}",
  ]
}

# wt-winnipeg

resource "aws_vpc_endpoint" "wt-winnipeg" {
  vpc_id            = "${var.vpc_id}"
  service_name      = "${var.service-wt-winnipeg}"
  vpc_endpoint_type = "Interface"

  security_group_ids = [
    "${aws_security_group.interservice.id}",
  ]

  subnet_ids = [
    "${var.subnets_ids}",
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

data "aws_route53_zone" "weco_zone" {
  provider = aws.dns
  name     = "wellcomecollection.org."
}

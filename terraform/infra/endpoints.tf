resource "aws_route53_zone" "internal" {
  name = "storage.internal."

  vpc {
    vpc_id = local.vpc_id
  }

  tags = local.default_tags
}


# TODO: Do we actually need this?  Is it used for anything?
resource "aws_route53_zone" "internal" {
  name = "storage.internal."

  vpc {
    vpc_id = local.storage_vpcs["storage_vpc_id"]
  }
}

data "aws_subnet" "private_new" {
  count = "3"
  id    = "${element(local.private_subnets, count.index)}"
}

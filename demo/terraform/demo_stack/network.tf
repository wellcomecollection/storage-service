module "vpc" {
  source = "github.com/wellcomecollection/platform-infrastructure.git//accounts/modules/vpc?ref=fc1a3cf"

  vpc_name   = var.namespace
  cidr_block = var.cidr_block
}

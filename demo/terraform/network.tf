module "vpc" {
  source = "github.com/wellcomecollection/platform-infrastructure.git//accounts/modules/vpc?ref=fc1a3cf"

  vpc_name   = local.namespace
  cidr_block = local.cidr_block
}

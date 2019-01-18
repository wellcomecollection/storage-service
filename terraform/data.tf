data "aws_subnet" "private_new" {
  count = "3"
  id    = "${element(local.private_subnets, count.index)}"
}

# Release params

data "aws_ssm_parameter" "archivist_image" {
  name     = "/releases/storage/latest/archivist"
  provider = "aws.platform"
}

data "aws_ssm_parameter" "bags_image" {
  name     = "/releases/storage/latest/bags"
  provider = "aws.platform"
}

data "aws_ssm_parameter" "bags_api_image" {
  name     = "/releases/storage/latest/bags_api"
  provider = "aws.platform"
}

data "aws_ssm_parameter" "ingests_image" {
  name     = "/releases/storage/latest/ingests"
  provider = "aws.platform"
}

data "aws_ssm_parameter" "ingests_api_image" {
  name     = "/releases/storage/latest/ingests_api"
  provider = "aws.platform"
}

data "aws_ssm_parameter" "notifier_image" {
  name     = "/releases/storage/latest/notifier"
  provider = "aws.platform"
}

data "aws_ssm_parameter" "bagger_image" {
  name     = "/releases/storage/latest/bagger"
  provider = "aws.platform"
}

data "aws_ssm_parameter" "bag_replicator_image" {
  name     = "/releases/storage/latest/bag_replicator"
  provider = "aws.platform"
}

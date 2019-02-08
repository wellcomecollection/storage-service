module "images" {
  source = "git::https://github.com/wellcometrust/terraform.git//ecs/modules/images?ref=v19.8.0"

  label   = "${var.release_label}"
  project = "storage"

  services = [
    "archivist",
    "bags",
    "bags_api",
    "ingests",
    "ingests_api",
    "notifier",
    "bagger",
    "bag_replicator",
  ]

  providers = {
    aws = "aws.platform"
  }
}

locals {
  archivist_image      = "${module.images.services["archivist"]}"
  bags_image           = "${module.images.services["bags"]}"
  bags_api_image       = "${module.images.services["bags_api"]}"
  ingests_image        = "${module.images.services["ingests"]}"
  ingests_api_image    = "${module.images.services["ingests_api"]}"
  notifier_image       = "${module.images.services["notifier"]}"
  bagger_image         = "${module.images.services["bagger"]}"
  bag_replicator_image = "${module.images.services["bag_replicator"]}"
}

provider "aws" {
  alias = "platform"

  assume_role {
    role_arn = "arn:aws:iam::760097843905:role/developer"
  }

  region  = "${var.aws_region}"
  version = "1.55.0"
}

module "images" {
  source = "git::https://github.com/wellcometrust/terraform.git//ecs/modules/images?ref=v19.8.0"

  label   = "${var.release_label}"
  project = "storage"

  services = [
    "archivist",
    "bags_api",
    "ingests",
    "ingests_api",
    "notifier",
    "bagger",
    "bag_register",
    "bag_replicator",
    "bag_verifier",
    "bag_unpacker",
  ]
}

locals {
  archivist_image      = "${module.images.services["archivist"]}"
  bag_register_image   = "${module.images.services["bag_register"]}"
  bags_api_image       = "${module.images.services["bags_api"]}"
  ingests_image        = "${module.images.services["ingests"]}"
  ingests_api_image    = "${module.images.services["ingests_api"]}"
  notifier_image       = "${module.images.services["notifier"]}"
  bagger_image         = "${module.images.services["bagger"]}"
  bag_replicator_image = "${module.images.services["bag_replicator"]}"
  bag_verifier_image   = "${module.images.services["bag_verifier"]}"
  bag_unpacker_image   = "${module.images.services["bag_unpacker"]}"
}

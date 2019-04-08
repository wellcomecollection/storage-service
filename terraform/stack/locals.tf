locals {
  bags_api_service_name    = "${var.namespace}-bags-api"
  ingests_service_name     = "${var.namespace}-ingests"
  ingests_api_service_name = "${var.namespace}-ingests-api"
  notifier_service_name    = "${var.namespace}-notifier"
  bagger_service_name      = "${var.namespace}-bagger"

  bag_unpacker_service_name           = "${var.namespace}-bag-unpacker"
  bag_replicator_service_name         = "${var.namespace}-bag-replicator"
  bag_register_service_name           = "${var.namespace}-bag-register"
  bag_verifier_post_repl_service_name = "${var.namespace}-bag-verifier-post-replication"
  bag_verifier_pre_repl_service_name  = "${var.namespace}-bag-verifier-pre-replication"

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

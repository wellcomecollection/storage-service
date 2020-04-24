locals {
  bags_api_service_name    = "${var.namespace}-bags-api"
  ingests_service_name     = "${var.namespace}-ingests"
  ingests_api_service_name = "${var.namespace}-ingests-api"
  notifier_service_name    = "${var.namespace}-notifier"

  bag_unpacker_service_name           = "${var.namespace}-bag-unpacker"
  bag_root_finder_service_name        = "${var.namespace}-bag-root-finder"
  bag_versioner_service_name          = "${var.namespace}-bag-versioner"
  bag_replicator_service_name         = "${var.namespace}-bag-replicator"
  bag_register_service_name           = "${var.namespace}-bag-register"
  bag_verifier_post_repl_service_name = "${var.namespace}-bag-verifier-post-replication"
  bag_verifier_pre_repl_service_name  = "${var.namespace}-bag-verifier-pre-replication"
  replica_aggregator_service_name     = "${var.namespace}-replica_aggregator"

  bag_versioner_image      = local.image_ids["bag_versioner"]
  bag_register_image       = local.image_ids["bag_register"]
  bag_root_finder_image    = local.image_ids["bag_root_finder"]
  bags_api_image           = local.image_ids["bags_api"]
  ingests_image            = local.image_ids["ingests"]
  ingests_api_image        = local.image_ids["ingests_api"]
  notifier_image           = local.image_ids["notifier"]
  bag_replicator_image     = local.image_ids["bag_replicator"]
  bag_verifier_image       = local.image_ids["bag_verifier"]
  bag_unpacker_image       = local.image_ids["bag_unpacker"]
  replica_aggregator_image = local.image_ids["replica_aggregator"]

  java_opts_heap_size = "-Xss6M -Xms2G -Xmx3G"

  service_discovery_namespace_id = aws_service_discovery_private_dns_namespace.namespace.id
}


locals {
  bags_api_service_name    = "${var.namespace}-bags-api"
  ingests_service_name     = "${var.namespace}-ingests-service"
  ingests_api_service_name = "${var.namespace}-ingests-api"
  notifier_service_name    = "${var.namespace}-notifier"

  bag_unpacker_service_name          = "${var.namespace}-bag-unpacker"
  bag_root_finder_service_name       = "${var.namespace}-bag-root-finder"
  bag_tagger_service_name            = "${var.namespace}-bag-tagger"
  bag_versioner_service_name         = "${var.namespace}-bag-versioner"
  bag_register_service_name          = "${var.namespace}-bag-register"
  bag_verifier_pre_repl_service_name = "${var.namespace}-bag-verifier-pre-replication"
  bag_indexer_service_name           = "${var.namespace}-bag_indexer"
  file_finder_service_name           = "${var.namespace}-file_finder"
  file_indexer_service_name          = "${var.namespace}-file_indexer"
  ingests_indexer_service_name       = "${var.namespace}-ingests_indexer"
  replica_aggregator_service_name    = "${var.namespace}-replica_aggregator"

  service_discovery_namespace_id = aws_service_discovery_private_dns_namespace.namespace.id

  ingests_listener_port = "65535"
  bags_listener_port    = "65534"

  java_opts_heap_size = "-Xss6M -Xms2G -Xmx3G"
}
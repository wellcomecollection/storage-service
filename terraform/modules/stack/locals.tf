locals {
  bags_api_service_name    = "${var.namespace}-bags-api"
  ingests_service_name     = "${var.namespace}-ingests-service"
  ingests_api_service_name = "${var.namespace}-ingests-api"
  notifier_service_name    = "${var.namespace}-notifier"

  bag_unpacker_service_name           = "${var.namespace}-bag-unpacker"
  bag_root_finder_service_name        = "${var.namespace}-bag-root-finder"
  bag_versioner_service_name          = "${var.namespace}-bag-versioner"
  bag_replicator_service_name         = "${var.namespace}-bag-replicator"
  bag_register_service_name           = "${var.namespace}-bag-register"
  bag_verifier_post_repl_service_name = "${var.namespace}-bag-verifier-post-replication"
  bag_verifier_pre_repl_service_name  = "${var.namespace}-bag-verifier-pre-replication"
  ingests_indexer_service_name        = "${var.namespace}-ingests_indexer"
  replica_aggregator_service_name     = "${var.namespace}-replica_aggregator"

  logstash_transit_service_name = "${var.namespace}_logstash_transit"
  logstash_transit_image        = "wellcome/logstash_transit:edgelord"
  logstash_host                 = "${local.logstash_transit_service_name}.${var.namespace}"

  service_discovery_namespace_id = aws_service_discovery_private_dns_namespace.namespace.id

  ingests_listener_port = "65535"
  bags_listener_port    = "65534"

  java_opts_heap_size = "-Xss6M -Xms2G -Xmx3G"
}
locals {
  services = [
    "bags_api",
    "ingests_api",
    "ingests_indexer",
    "ingests_tracker",
    "ingests_worker",
    "notifier",
    "bag_versioner",
    "bag_register",
    "bag_tagger",
    "bag_replicator",
    "bag_root_finder",
    "bag_tracker",
    "bag_verifier",
    "bag_unpacker",
    "bag_indexer",
    "file_finder",
    "file_indexer",
    "replica_aggregator",
  ]
}

locals {
  repo_urls = [
    for name in local.services :
    "${var.app_containers["container_registry"]}/${name}:${var.app_containers["container_tag"]}"
  ]
  image_ids = zipmap(local.services, local.repo_urls)
}

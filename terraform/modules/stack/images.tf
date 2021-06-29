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
    "975596993436.dkr.ecr.eu-west-1.amazonaws.com/uk.ac.wellcome/${name}:env.${var.release_label}"
  ]
  image_ids = zipmap(local.services, local.repo_urls)
}

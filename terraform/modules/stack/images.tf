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
    "replica_aggregator",
  ]
}

data "aws_ssm_parameter" "image_ids" {
  count = length(local.services)

  name = "/storage/images/${var.release_label}/${local.services[count.index]}"
}

locals {
  image_ids = zipmap(local.services, data.aws_ssm_parameter.image_ids.*.value)
}

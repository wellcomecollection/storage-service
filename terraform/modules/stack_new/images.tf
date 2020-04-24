locals {
  services = [
    "bags_api",
    "ingests",
    "ingests_api",
    "notifier",
    "bag_versioner",
    "bag_register",
    "bag_replicator",
    "bag_root_finder",
    "bag_verifier",
    "bag_unpacker",
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

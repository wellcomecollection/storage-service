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

data "aws_ecr_repository" "service" {
  count = length(local.services)
  name  = "uk.ac.wellcome/${local.services[count.index]}"
}

locals {
  repo_urls = [for repo_url in data.aws_ecr_repository.service.*.repository_url : "${repo_url}:env.${var.release_label}"]
  image_ids = zipmap(local.services, local.repo_urls)
}

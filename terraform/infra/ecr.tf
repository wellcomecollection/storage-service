locals {
  repository_names = [
    "bags_api",
    "bag_indexer",
    "bag_replicator",
    "bag_root_finder",
    "bag_tagger",
    "bag_tracker",
    "bag_unpacker",
    "bag_verifier",
    "bag_versioner",
    "file_finder",
    "file_indexer",
    "ingests_api",
    "ingests_indexer",
    "ingests_tracker",
    "ingests_worker",
    "notifier",
    "replica_aggregator",
  ]
}

resource "aws_ecr_repository" "private" {
  count = length(local.repository_names)
  name  = "uk.ac.wellcome/${local.repository_names[count.index]}"
}

resource "aws_ecrpublic_repository" "public" {
  provider = aws.us_east_1

  count           = length(local.repository_names)
  repository_name = local.repository_names[count.index]
}

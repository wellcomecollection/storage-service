resource "aws_ecr_repository" "bags_api" {
  name = "uk.ac.wellcome/bags_api"
  tags = local.default_tags
}

resource "aws_ecr_repository" "bag_indexer" {
  name = "uk.ac.wellcome/bag_indexer"
  tags = local.default_tags
}

resource "aws_ecr_repository" "bag_register" {
  name = "uk.ac.wellcome/bags"
  tags = local.default_tags
}

resource "aws_ecr_repository" "bag_replicator" {
  name = "uk.ac.wellcome/bag_replicator"
  tags = local.default_tags
}

resource "aws_ecr_repository" "bag_root_finder" {
  name = "uk.ac.wellcome/bag_root_finder"
  tags = local.default_tags
}

resource "aws_ecr_repository" "bag_tracker" {
  name = "uk.ac.wellcome/bag_tracker"
  tags = local.default_tags
}

resource "aws_ecr_repository" "bag_unpacker" {
  name = "uk.ac.wellcome/bag_unpacker"
  tags = local.default_tags
}

resource "aws_ecr_repository" "bag_verifier" {
  name = "uk.ac.wellcome/bag_verifier"
  tags = local.default_tags
}

resource "aws_ecr_repository" "bag_versioner" {
  name = "uk.ac.wellcome/bag_versioner"
  tags = local.default_tags
}

resource "aws_ecr_repository" "ingests_api" {
  name = "uk.ac.wellcome/ingests_api"
  tags = local.default_tags
}

resource "aws_ecr_repository" "ingests_indexer" {
  name = "uk.ac.wellcome/ingests_indexer"
  tags = local.default_tags
}

resource "aws_ecr_repository" "ingests_tracker" {
  name = "uk.ac.wellcome/ingests_tracker"
  tags = local.default_tags
}

resource "aws_ecr_repository" "ingests_worker" {
  name = "uk.ac.wellcome/ingests_worker"
  tags = local.default_tags
}

resource "aws_ecr_repository" "nginx" {
  name = "uk.ac.wellcome/nginx"
  tags = local.default_tags
}

resource "aws_ecr_repository" "notifier" {
  name = "uk.ac.wellcome/notifier"
  tags = local.default_tags
}

resource "aws_ecr_repository" "replica_aggregator" {
  name = "uk.ac.wellcome/replica_aggregator"
  tags = local.default_tags
}

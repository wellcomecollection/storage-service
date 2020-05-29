resource "aws_ecr_repository" "bags_api" {
  name = "uk.ac.wellcome/bags_api"
}

resource "aws_ecr_repository" "ingests_api" {
  name = "uk.ac.wellcome/ingests_api"
}

resource "aws_ecr_repository" "notifier" {
  name = "uk.ac.wellcome/notifier"
}

resource "aws_ecr_repository" "bag_replicator" {
  name = "uk.ac.wellcome/bag_replicator"
}

resource "aws_ecr_repository" "bag_root_finder" {
  name = "uk.ac.wellcome/bag_root_finder"
}

resource "aws_ecr_repository" "bag_verifier" {
  name = "uk.ac.wellcome/bag_verifier"
}

resource "aws_ecr_repository" "bag_unpacker" {
  name = "uk.ac.wellcome/bag_unpacker"
}

resource "aws_ecr_repository" "bag_register" {
  name = "uk.ac.wellcome/bags"
}

resource "aws_ecr_repository" "bag_indexer" {
  name = "uk.ac.wellcome/bag_indexer"
}

resource "aws_ecr_repository" "bag_tracker" {
  name = "uk.ac.wellcome/bag_tracker"
}

resource "aws_ecr_repository" "bag_versioner" {
  name = "uk.ac.wellcome/bag_versioner"
}

resource "aws_ecr_repository" "ingests_indexer" {
  name = "uk.ac.wellcome/ingests_indexer"
}

resource "aws_ecr_repository" "ingests_tracker" {
  name = "uk.ac.wellcome/ingests_tracker"
}

resource "aws_ecr_repository" "ingests_worker" {
  name = "uk.ac.wellcome/ingests_worker"
}

resource "aws_ecr_repository" "replica_aggregator" {
  name = "uk.ac.wellcome/replica_aggregator"
}

resource "aws_ecr_repository" "nginx" {
  name = "uk.ac.wellcome/nginx"
}

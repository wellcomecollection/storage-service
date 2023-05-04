locals {
  repository_names = [
    "bags_api",
    "bag_indexer",
    "bag_register",
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

  ecr_policy_only_keep_the_last_100_images = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Only keep the last 100 images in a repo"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 100
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

resource "aws_ecr_repository" "private" {
  for_each = toset(local.repository_names)

  name = "uk.ac.wellcome/${each.key}"
}

resource "aws_ecr_lifecycle_policy" "private" {
  for_each = aws_ecr_repository.private

  repository = each.value.name
  policy     = local.ecr_policy_only_keep_the_last_100_images
}

resource "aws_ecrpublic_repository" "public" {
  provider = aws.us_east_1

  for_each = toset(local.repository_names)

  repository_name = each.key
}

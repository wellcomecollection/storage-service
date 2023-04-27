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

moved {
  from = aws_ecr_repository.private[0]
  to   = aws_ecr_repository.private["bags_api"]
}
moved {
  from = aws_ecr_repository.private[1]
  to   = aws_ecr_repository.private["bag_indexer"]
}
moved {
  from = aws_ecr_repository.private[2]
  to   = aws_ecr_repository.private["bag_register"]
}
moved {
  from = aws_ecr_repository.private[3]
  to   = aws_ecr_repository.private["bag_replicator"]
}
moved {
  from = aws_ecr_repository.private[4]
  to   = aws_ecr_repository.private["bag_root_finder"]
}
moved {
  from = aws_ecr_repository.private[5]
  to   = aws_ecr_repository.private["bag_tagger"]
}
moved {
  from = aws_ecr_repository.private[6]
  to   = aws_ecr_repository.private["bag_tracker"]
}
moved {
  from = aws_ecr_repository.private[7]
  to   = aws_ecr_repository.private["bag_unpacker"]
}
moved {
  from = aws_ecr_repository.private[8]
  to   = aws_ecr_repository.private["bag_verifier"]
}
moved {
  from = aws_ecr_repository.private[9]
  to   = aws_ecr_repository.private["bag_versioner"]
}
moved {
  from = aws_ecr_repository.private[10]
  to   = aws_ecr_repository.private["file_finder"]
}
moved {
  from = aws_ecr_repository.private[11]
  to   = aws_ecr_repository.private["file_indexer"]
}
moved {
  from = aws_ecr_repository.private[12]
  to   = aws_ecr_repository.private["ingests_api"]
}
moved {
  from = aws_ecr_repository.private[13]
  to   = aws_ecr_repository.private["ingests_indexer"]
}
moved {
  from = aws_ecr_repository.private[14]
  to   = aws_ecr_repository.private["ingests_tracker"]
}
moved {
  from = aws_ecr_repository.private[15]
  to   = aws_ecr_repository.private["ingests_worker"]
}
moved {
  from = aws_ecr_repository.private[16]
  to   = aws_ecr_repository.private["notifier"]
}
moved {
  from = aws_ecr_repository.private[17]
  to   = aws_ecr_repository.private["replica_aggregator"]
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

moved {
  from = aws_ecrpublic_repository.public[0]
  to   = aws_ecrpublic_repository.public["bags_api"]
}
moved {
  from = aws_ecrpublic_repository.public[1]
  to   = aws_ecrpublic_repository.public["bag_indexer"]
}
moved {
  from = aws_ecrpublic_repository.public[2]
  to   = aws_ecrpublic_repository.public["bag_register"]
}
moved {
  from = aws_ecrpublic_repository.public[3]
  to   = aws_ecrpublic_repository.public["bag_replicator"]
}
moved {
  from = aws_ecrpublic_repository.public[4]
  to   = aws_ecrpublic_repository.public["bag_root_finder"]
}
moved {
  from = aws_ecrpublic_repository.public[5]
  to   = aws_ecrpublic_repository.public["bag_tagger"]
}
moved {
  from = aws_ecrpublic_repository.public[6]
  to   = aws_ecrpublic_repository.public["bag_tracker"]
}
moved {
  from = aws_ecrpublic_repository.public[7]
  to   = aws_ecrpublic_repository.public["bag_unpacker"]
}
moved {
  from = aws_ecrpublic_repository.public[8]
  to   = aws_ecrpublic_repository.public["bag_verifier"]
}
moved {
  from = aws_ecrpublic_repository.public[9]
  to   = aws_ecrpublic_repository.public["bag_versioner"]
}
moved {
  from = aws_ecrpublic_repository.public[10]
  to   = aws_ecrpublic_repository.public["file_finder"]
}
moved {
  from = aws_ecrpublic_repository.public[11]
  to   = aws_ecrpublic_repository.public["file_indexer"]
}
moved {
  from = aws_ecrpublic_repository.public[12]
  to   = aws_ecrpublic_repository.public["ingests_api"]
}
moved {
  from = aws_ecrpublic_repository.public[13]
  to   = aws_ecrpublic_repository.public["ingests_indexer"]
}
moved {
  from = aws_ecrpublic_repository.public[14]
  to   = aws_ecrpublic_repository.public["ingests_tracker"]
}
moved {
  from = aws_ecrpublic_repository.public[15]
  to   = aws_ecrpublic_repository.public["ingests_worker"]
}
moved {
  from = aws_ecrpublic_repository.public[16]
  to   = aws_ecrpublic_repository.public["notifier"]
}
moved {
  from = aws_ecrpublic_repository.public[17]
  to   = aws_ecrpublic_repository.public["replica_aggregator"]
}

resource "aws_ecrpublic_repository" "public" {
  provider = aws.us_east_1

  for_each = toset(local.repository_names)

  repository_name = each.key
}

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

  # Keeps every image a deployment can reference (env.* and latest tags), then only the newest 10 others,
  # so Inspector scans what we run rather than months of CI builds.
  ecr_lifecycle_policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep env tagged images, e.g. env.prod or env.stage"
        selection = {
          tagStatus     = "tagged"
          tagPrefixList = ["env."]
          countType     = "imageCountMoreThan"
          countNumber   = 20
        }
        action = { type = "expire" }
      },
      {
        rulePriority = 2
        description  = "Keep the latest tagged image"
        selection = {
          tagStatus     = "tagged"
          tagPrefixList = ["latest"]
          countType     = "imageCountMoreThan"
          countNumber   = 1
        }
        action = { type = "expire" }
      },
      {
        rulePriority = 3
        description  = "Expire other tagged images, keep only the last 10"
        selection = {
          tagStatus      = "tagged"
          tagPatternList = ["*"]
          countType      = "imageCountMoreThan"
          countNumber    = 10
        }
        action = { type = "expire" }
      },
      {
        rulePriority = 4
        description  = "Expire untagged images, keep only the last 5"
        selection = {
          tagStatus   = "untagged"
          countType   = "imageCountMoreThan"
          countNumber = 5
        }
        action = { type = "expire" }
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
  policy     = local.ecr_lifecycle_policy
}

resource "aws_ecrpublic_repository" "public" {
  provider = aws.us_east_1

  for_each = toset(local.repository_names)

  repository_name = each.key
}

provider "aws" {
  assume_role {
    role_arn = "arn:aws:iam::975596993436:role/storage-developer"
  }

  region  = "eu-west-1"
  version = "~> 2.7"
}

resource "aws_ecr_repository" "sha1_fetcher" {
  name = "uk.ac.wellcome/sha1_fetcher"
}

# queue

data "terraform_remote_state" "infra_shared" {
  backend = "s3"

  config = {
    role_arn = "arn:aws:iam::760097843905:role/platform-read_only"
    bucket   = "wellcomecollection-platform-infra"
    key      = "terraform/platform-infrastructure/shared.tfstate"
    region   = "eu-west-1"
  }
}

module "queue" {
  source = "../terraform/modules/queue"

  name = "sha1_fetcher_input"

  topic_arns = []

  dlq_alarm_arn = data.terraform_remote_state.infra_shared.outputs.dlq_alarm_arn

  visibility_timeout_seconds = 60 * 30 # 30 minutes

  aws_region = "eu-west-1"

  role_names = [
    module.worker.task_role_name
  ]

  queue_high_actions = [
    module.worker.scale_up_arn,
  ]

  queue_low_actions = [
    module.worker.scale_down_arn,
  ]
}

output "queue_url" {
  value = module.queue.url
}

## worker

resource "aws_ecs_cluster" "cluster" {
  name = "sha1_fetcher"
}

resource "aws_service_discovery_private_dns_namespace" "namespace" {
  name = "sha1_fetcher"
  vpc  = data.terraform_remote_state.infra_shared.outputs.storage_vpc_id
}

module "worker" {
  source = "../terraform/modules/service/scaling_worker"

  env_vars = {}

  subnets = data.terraform_remote_state.infra_shared.outputs.storage_vpc_private_subnets

  container_image = "975596993436.dkr.ecr.eu-west-1.amazonaws.com/uk.ac.wellcome/sha1_fetcher:latest"

  namespace_id = aws_service_discovery_private_dns_namespace.namespace.id

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn

  service_name = "sha1_fetcher"

  min_capacity = 0
  max_capacity = 5

  security_group_ids = []

  cpu    = 1024
  memory = 2048

  use_fargate_spot = true
}

# permissions

data "aws_iam_policy_document" "worker_permissions" {
  statement {
    actions = [
      "s3:Get*",
      "dynamodb:Get*",
    ]

    resources = [
      "*"
    ]
  }

  statement {
    actions = [
      "s3:Put*",
    ]

    resources = [
      "arn:aws:s3:::wellcomecollection-storage-infra/sha1/*",
    ]
  }
}

resource "aws_iam_role_policy" "bag_verifier_pre_repl_metrics" {
  role   = module.worker.task_role_name
  policy = data.aws_iam_policy_document.worker_permissions.json
}
locals {
  repository_names = [
    aws_ecr_repository.bags_api.name,
    aws_ecr_repository.bag_indexer.name,
    aws_ecr_repository.bag_register.name,
    aws_ecr_repository.bag_replicator.name,
    aws_ecr_repository.bag_root_finder.name,
    aws_ecr_repository.bag_tagger.name,
    aws_ecr_repository.bag_tracker.name,
    aws_ecr_repository.bag_unpacker.name,
    aws_ecr_repository.bag_verifier.name,
    aws_ecr_repository.bag_versioner.name,
    aws_ecr_repository.file_finder.name,
    aws_ecr_repository.file_indexer.name,
    aws_ecr_repository.ingests_api.name,
    aws_ecr_repository.ingests_indexer.name,
    aws_ecr_repository.ingests_tracker.name,
    aws_ecr_repository.ingests_worker.name,
    aws_ecr_repository.nginx.name,
    aws_ecr_repository.notifier.name,
    aws_ecr_repository.replica_aggregator.name,
  ]

  account_ids = {
    dams_prototype = "241906670800"
  }
}

# This policy allows services running in other AWS accounts to read our
# ECR images.  Eventually we should move to using public ECR repositories,
# but that's more work than just allowing cross-account access for now.
#
# https://aws.amazon.com/premiumsupport/knowledge-center/secondary-account-access-ecr/
#
resource "aws_ecr_repository_policy" "allow_cross_account_access" {
  count = length(local.repository_names)

  repository = local.repository_names[count.index]
  policy = <<EOF
{
    "Version": "2008-10-17",
    "Statement": [
        {
            "Sid": "AllowCrossAccountPull",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::${local.account_ids["dams_prototype"]}:root"
            },
            "Action": [
                "ecr:GetDownloadUrlForLayer",
                "ecr:BatchGetImage",
                "ecr:BatchCheckLayerAvailability"
            ]
        }
    ]
}
EOF
}

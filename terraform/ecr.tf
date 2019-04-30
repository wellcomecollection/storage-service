module "ecr_repository_bags_api" {
  source    = "git::https://github.com/wellcometrust/terraform.git//ecr?ref=v19.5.1"
  id        = "bags_api"
  namespace = "uk.ac.wellcome"
}

module "ecr_repository_ingests" {
  source    = "git::https://github.com/wellcometrust/terraform.git//ecr?ref=v19.5.1"
  id        = "ingests"
  namespace = "uk.ac.wellcome"
}

module "ecr_repository_ingests_api" {
  source    = "git::https://github.com/wellcometrust/terraform.git//ecr?ref=v19.5.1"
  id        = "ingests_api"
  namespace = "uk.ac.wellcome"
}

module "ecr_repository_notifier" {
  source    = "git::https://github.com/wellcometrust/terraform.git//ecr?ref=v19.5.1"
  id        = "notifier"
  namespace = "uk.ac.wellcome"
}

module "ecr_repository_bag_replicator" {
  source    = "git::https://github.com/wellcometrust/terraform.git//ecr?ref=v19.5.1"
  id        = "bag_replicator"
  namespace = "uk.ac.wellcome"
}

module "ecr_repository_bag_verifier" {
  source    = "git::https://github.com/wellcometrust/terraform.git//ecr?ref=v19.5.1"
  id        = "bag_verifier"
  namespace = "uk.ac.wellcome"
}

module "ecr_repository_bag_unpacker" {
  source    = "git::https://github.com/wellcometrust/terraform.git//ecr?ref=v19.5.1"
  id        = "bag_unpacker"
  namespace = "uk.ac.wellcome"
}

module "ecr_repository_bag_register" {
  source    = "git::https://github.com/wellcometrust/terraform.git//ecr?ref=v19.5.1"
  id        = "bags"
  namespace = "uk.ac.wellcome"
}

module "ecr_repository_bagger" {
  source    = "git::https://github.com/wellcometrust/terraform.git//ecr?ref=v19.5.1"
  id        = "bagger"
  namespace = "uk.ac.wellcome"
}

module "ecr_repository_bag_auditor" {
  source    = "git::https://github.com/wellcometrust/terraform.git//ecr?ref=v19.5.1"
  id        = "bag_auditor"
  namespace = "uk.ac.wellcome"
}

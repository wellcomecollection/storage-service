module "demo_stack" {
  source = "github.com/wellcomecollection/storage-service.git//demo/terraform/demo_stack?ref=2cf78b5"

  namespace       = "weco-dams-prototype"
  short_namespace = "weco"
}

output "elasticsearch_host" { value = module.demo_stack.elasticsearch_host }
output "kibana_endpoint" { value = module.demo_stack.kibana_endpoint }
output "token_url" { value = module.demo_stack.token_url }
output "api_url" { value = module.demo_stack.api_url }
output "replica_primary_bucket_name" { value = module.demo_stack.replica_primary_bucket_name }
output "replica_glacier_bucket_name" { value = module.demo_stack.replica_glacier_bucket_name }
output "uploads_bucket_name" { value = module.demo_stack.uploads_bucket_name }
output "unpacked_bags_bucket_name" { value = module.demo_stack.unpacked_bags_bucket_name }

provider "aws" {
  assume_role {
    role_arn = "arn:aws:iam::241906670800:role/dam_prototype-admin"
  }

  region = "eu-west-3"
}

terraform {
  backend "s3" {
    role_arn = "arn:aws:iam::975596993436:role/storage-developer"

    bucket         = "wellcomecollection-storage-infra"
    key            = "terraform/dams-prototype-project/main.tfstate"
    dynamodb_table = "terraform-locktable"
    region         = "eu-west-1"
  }
}

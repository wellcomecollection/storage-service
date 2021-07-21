provider "aws" {
  assume_role {
    role_arn = "arn:aws:iam::241906670800:role/dam_prototype-admin"
  }

  region = "eu-west-3"
}




module "us_demo_stack" {
  source = "github.com/wellcomecollection/storage-service.git//demo/terraform/demo_stack?ref=6694a81"

  namespace       = "weco-us1-dams-prototype"
  short_namespace = "weco-us1"

  providers = {
    aws = aws.us_east_1
  }
}

output "us_elasticsearch_host" { value = module.us_demo_stack.elasticsearch_host }
output "us_kibana_endpoint" { value = module.us_demo_stack.kibana_endpoint }
output "us_token_url" { value = module.us_demo_stack.token_url }
output "us_api_url" { value = module.us_demo_stack.api_url }
output "us_replica_primary_bucket_name" { value = module.us_demo_stack.replica_primary_bucket_name }
output "us_replica_glacier_bucket_name" { value = module.us_demo_stack.replica_glacier_bucket_name }
output "us_uploads_bucket_name" { value = module.us_demo_stack.uploads_bucket_name }
output "us_unpacked_bags_bucket_name" { value = module.us_demo_stack.unpacked_bags_bucket_name }

provider "aws" {
  alias = "us_east_1"

  assume_role {
    role_arn = "arn:aws:iam::241906670800:role/dam_prototype-admin"
  }

  region = "us-east-1"
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

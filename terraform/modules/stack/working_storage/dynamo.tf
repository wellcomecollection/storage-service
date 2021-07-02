module "versioner_lock_table" {
  source = "../../lock_table"

  namespace = var.namespace
  owner     = "versioner"
}

module "replicator_lock_table" {
  source = "../../lock_table"

  namespace = var.namespace
  owner     = "replicator"
}

resource "aws_dynamodb_table" "azure_verifier_tags" {
  count = var.azure_replicator_enabled ? 1 : 0

  name     = "${var.namespace}_azure_verifier_tags"
  hash_key = "id"

  billing_mode = "PAY_PER_REQUEST"

  attribute {
    name = "id"
    type = "S"
  }
}

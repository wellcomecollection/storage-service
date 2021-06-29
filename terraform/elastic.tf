# We use Elasticsearch for two purposes in the storage service:
#
#   1) For application logs
#   2) For storage service data that we can search on, e.g. bags and files
#
# In the Wellcome installation, these are two separate clusters hosted
# in Elastic Cloud.  Each customer has multiple users with per-user permissions.
#
# For the prototype, we're using a single AWS Elasticsearch instance with
# the superuser account.  This allows us to bootstrap quickly, but you
# shouldn't use this in production.

resource "random_password" "elasticsearch" {
  length           = 32
  special          = true
  override_special = "_%@"
}

locals {
  elasticsearch_user     = "root"
  elasticsearch_password = random_password.elasticsearch.result
}

module "elasticsearch_secrets" {
  source = "github.com/wellcomecollection/storage-service.git//terraform/modules/secrets?ref=b24ea38"

  key_value_map = {
    "elasticsearch_user"     = local.elasticsearch_user
    "elasticsearch_password" = local.elasticsearch_password

    "elasticsearch/endpoint" = aws_elasticsearch_domain.elasticsearch.endpoint
    "elasticsearch/protocol" = "https"
    "elasticsearch/port"     = 443
  }
}

resource "aws_elasticsearch_domain" "elasticsearch" {
  domain_name = local.namespace

  elasticsearch_version = "7.10"

  cluster_config {
    instance_type  = "r4.large.elasticsearch"
    instance_count = 1
  }

  advanced_security_options {
    enabled                        = true
    internal_user_database_enabled = true

    master_user_options {
      master_user_name     = local.elasticsearch_user
      master_user_password = local.elasticsearch_password
    }
  }

  access_policies = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "*"
      },
      "Action": "es:*",
      "Resource": "arn:aws:es:${local.aws_region}:${local.account_id}:domain/${local.namespace}/*"
    }
  ]
}
EOF

  # You have to use EBS with the "r4.large.elasticsearch" instance type,
  # or Terraform returns an error.
  ebs_options {
    ebs_enabled = true
    volume_size = 100
  }

  # Node-to-node encryption, encryption at rest and enforced HTTPS are
  # all requirements for the "advanced security".
  node_to_node_encryption {
    enabled = true
  }

  encrypt_at_rest {
    enabled = true
  }

  domain_endpoint_options {
    enforce_https = true

    tls_security_policy = "Policy-Min-TLS-1-2-2019-07"
  }

  snapshot_options {
    automated_snapshot_start_hour = 23
  }
}

output "host" {
  value = aws_elasticsearch_domain.elasticsearch.endpoint
}

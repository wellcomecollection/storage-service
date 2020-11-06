data "aws_iam_policy_document" "role_assumer" {
  statement {
    effect = "Allow"

    actions = [
      "sts:AssumeRole",
    ]

    resources = local.assumable_roles
  }
}

data "aws_iam_policy_document" "assume_role_policy" {
  statement {
    effect = "Allow"


    principals {
      type = "Service"
      identifiers = [
        "ec2.amazonaws.com"
      ]
    }

    actions = [
      "sts:AssumeRole",
    ]
  }
}

resource "aws_iam_instance_profile" "dev_instance_profile" {
  name = "dev_instance_profile"
  role = aws_iam_role.dev_instance_role.name
}

resource "aws_iam_role_policy" "role_assumer" {
  role   = aws_iam_role.dev_instance_role.name
  policy = data.aws_iam_policy_document.role_assumer.json
}

resource "aws_iam_role" "dev_instance_role" {
  name               = "dev_instance_role"
  assume_role_policy = data.aws_iam_policy_document.assume_role_policy.json

  tags = {
    DeveloperName = var.dev_name
    LastUpdated = timestamp()
  }
}

resource "aws_security_group" "allow_ssh_dev_ip" {
  name        = "allow_ssh_dev_ip"
  description = "Allow inbound SSH traffic to local public IP"
  vpc_id      = local.developer_vpc_id

  ingress {
    from_port = 22
    to_port = 22
    protocol = "TCP"
    cidr_blocks = ["${local.ifconfig_co_json.ip}/32"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    DeveloperName = var.dev_name
    LastUpdated = timestamp()
  }
}

data "http" "my_public_ip" {
  url = "https://ifconfig.co/json"
  request_headers = {
    Accept = "application/json"
  }
}

locals {
  developer_vpc_id = data.terraform_remote_state.accounts_platform.outputs.developer_vpc_id
  ifconfig_co_json = jsondecode(data.http.my_public_ip.body)
  assumable_roles = [
    "arn:aws:iam::975596993436:role/storage-read_only",
    "arn:aws:iam::299497370133:role/workflow-developer",
    "arn:aws:iam::760097843905:role/platform-read_only"
  ]
}

data "terraform_remote_state" "accounts_platform" {
  backend = "s3"

  config = {
    role_arn = "arn:aws:iam::760097843905:role/platform-read_only"
    bucket   = "wellcomecollection-platform-infra"
    key      = "terraform/platform-infrastructure/accounts/platform.tfstate"
    region   = "eu-west-1"
  }
}

variable "dev_name" {
  default = "Robert Kenny"
}

output "my_ip_addr" {
  value = local.ifconfig_co_json.ip
}

output "security_group_id" {
  value = aws_security_group.allow_ssh_dev_ip.id
}

output "dev_instance_profile_name" {
  value = aws_iam_instance_profile.dev_instance_profile.name
}
resource "aws_iam_role" "assumable_read_role" {
  count = length(var.read_principals) > 0 ? 1 : 0

  name               = "${var.name}-assumable_read_role"
  assume_role_policy = data.aws_iam_policy_document.assume_read_role.json
}

data "aws_iam_policy_document" "assume_read_role" {
  statement {
    actions = [
      "sts:AssumeRole",
    ]

    principals {
      identifiers = var.read_principals
      type        = "AWS"
    }
  }
}

resource "aws_iam_role_policy" "read_role_policy" {
  count = length(var.read_principals) > 0 ? 1 : 0

  role   = aws_iam_role.assumable_read_role[0].id
  policy = data.aws_iam_policy_document.read_policy.json
}

data "aws_iam_policy_document" "cloudwatch_putmetrics" {
  statement {
    actions = [
      "cloudwatch:PutMetricData",
    ]

    resources = [
      "*",
    ]
  }
}

resource "aws_iam_role_policy" "bags_api_metrics" {
  role   = module.task_definition.task_role_name
  policy = data.aws_iam_policy_document.cloudwatch_putmetrics.json
}

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

# Lets a worker mark its task as protected while it holds a message,
# so autoscaling scale-in can't kill it mid-work.
data "aws_iam_policy_document" "ecs_task_protection" {
  statement {
    actions = [
      "ecs:UpdateTaskProtection",
    ]

    resources = [
      "${replace(var.cluster_arn, ":cluster/", ":task/")}/*",
    ]
  }
}

resource "aws_iam_role_policy" "ecs_task_protection" {
  role   = module.task_definition.task_role_name
  policy = data.aws_iam_policy_document.ecs_task_protection.json
}

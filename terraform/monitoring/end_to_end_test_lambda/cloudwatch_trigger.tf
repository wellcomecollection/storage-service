# This allows you to set up a rule that triggers a new bag ingest every day
# at 9pm.  It's currently disabled, but you can enable it by changing
# `count` to 1.

resource "aws_cloudwatch_event_rule" "every_day_at_9pm" {
  count = 0

  name                = "trigger-${var.name}"
  schedule_expression = "cron(* 21 * * ? *)"
}


resource "aws_lambda_permission" "allow_cloudwatch_trigger" {
  count = 0

  action        = "lambda:InvokeFunction"
  function_name = module.lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.every_day_at_9pm.arn
}

resource "aws_cloudwatch_event_target" "event_trigger" {
  count = 0

  rule  = aws_cloudwatch_event_rule.every_day_at_9pm.name
  arn   = module.lambda.arn
}

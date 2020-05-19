resource "aws_cloudwatch_event_rule" "every_day_at_9pm" {
  name                = "trigger-${var.name}"
  schedule_expression = "cron(* 21 * * ? *)"
}


resource "aws_lambda_permission" "allow_cloudwatch_trigger" {
  action        = "lambda:InvokeFunction"
  function_name = module.lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.every_day_at_9pm.arn
}

resource "aws_cloudwatch_event_target" "event_trigger" {
  rule  = aws_cloudwatch_event_rule.every_day_at_9pm.name
  arn   = module.lambda.arn
}


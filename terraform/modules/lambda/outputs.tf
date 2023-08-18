output "arn" {
  description = "ARN of the Lambda function"
  value       = module.lambda.lambda.arn
}

output "function_name" {
  description = "Name of the Lambda function"
  value       = module.lambda.lambda.function_name
}

output "role_name" {
  description = "Name of the IAM role for this Lambda"
  value       = module.lambda.lambda_role.name
}

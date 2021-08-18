variable "name" {
  type = string
}

variable "description" {
  type = string
}

variable "environment" {
  type = map(string)
}

variable "lambda_error_alerts_topic_arn" {
  type = string
}

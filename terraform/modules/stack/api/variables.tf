variable "subnets" {
  type = list(string)
}

variable "namespace" {
  type = string
}

variable "cognito_user_pool_arn" {
  type = string
}

variable "auth_scopes" {
  type = list(string)
}

variable "alarm_topic_arn" {
  type = string
}

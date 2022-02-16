variable "name" {
}

variable "topic_arns" {
  type = list(string)
}

variable "dlq_alarm_arn" {
}

variable "role_names" {
  type = list(string)
}

variable "visibility_timeout_seconds" {
  default = "300"
}

variable "max_receive_count" {
  default = 3
}

variable "queue_high_actions" {
  default = []
  type    = list(string)
}

variable "queue_low_actions" {
  default = []
  type    = list(string)
}

variable "cooldown_period" {
  type    = string
  default = "1m"
}

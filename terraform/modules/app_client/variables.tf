variable "name" {
  type = string
}

variable "user_pool_id" {
  type = string
}

variable "allow_bags_access" {
  type = bool
}

variable "allow_ingests_access" {
  type = bool
}

variable "explicit_auth_flows" {
  default = null
}

variable "refresh_token_validity" {
  default = 30
}

variable "generate_secret" {
  default = true
}
variable "name" {
  type = string
}

variable "description" {
  type = string
}

variable "environment" {
  type = map(string)
}

variable "tags" {
  type = map(string)
}

variable "name" {
  description = "Name of the Lambda"
  type        = string
}

variable "module_name" {
  description = "Name of the Python module where the handler function lives"
  default     = ""
  type        = string
}

variable "description" {
  description = "Description of the Lambda function"
  type        = string
}

variable "environment" {
  description = "Environment variables to pass to the Lambda"
  type        = map(string)

  # environment cannot be empty so we need to pass at least one value
  default = {
    EMPTY_VARIABLE = ""
  }
}

variable "timeout" {
  description = "The amount of time your Lambda function has to run (seconds)"
  type        = number
}

variable "s3_bucket" {
  description = "The S3 bucket containing the function's deployment package"
  type        = string
}

variable "s3_key" {
  description = "The S3 key of the function's deployment package"
  type        = string
}

variable "memory_size" {
  default = 128
  type    = number
}

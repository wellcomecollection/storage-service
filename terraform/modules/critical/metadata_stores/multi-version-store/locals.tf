locals {
  table_name  = var.table_name == "" ? "${var.table_name_prefix}${var.name}" : var.table_name
  bucket_name = var.bucket_name == "" ? "${var.bucket_name_prefix}${lower(var.name)}" : var.bucket_name
}
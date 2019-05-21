output "table_name" {
  value = "${aws_dynamodb_table.lock_table.name}"
}

output "index_name" {
  value = "${var.index_name}"
}

output "iam_policy" {
  value = "${data.aws_iam_policy_document.lock_table_readwrite.json}"
}

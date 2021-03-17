resource "aws_secretsmanager_secret" "secret" {
  for_each = var.key_value_map

  name = each.key
}

resource "aws_secretsmanager_secret_version" "secret" {
  for_each = var.key_value_map

  secret_id     = aws_secretsmanager_secret.secret[each.key].id
  secret_string = each.value
}

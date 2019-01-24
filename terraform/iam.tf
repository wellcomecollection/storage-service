resource "aws_iam_user" "dds_digirati" {
  name = "dds_digirati"
}

resource "aws_iam_access_key" "dds_digirati" {
  user    = "${aws_iam_user.dds_digirati.name}"
  pgp_key = "keybase:kenoir"
}

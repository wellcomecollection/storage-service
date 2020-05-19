locals {
  infra_bucket    = "wellcomecollection-storage-infra"
  test_bag_prefix = "test_bags/"
}

resource "aws_s3_bucket_object" "bag_with_one_text_file" {
  bucket = local.infra_bucket
  key    = "${local.test_bag_prefix}bag_with_one_text_file.tar.gz"
  source = "${path.module}/../../monitoring/test_bags/bag_with_one_text_file.tar.gz"

  etag = filemd5("${path.module}/../../monitoring/test_bags/bag_with_one_text_file.tar.gz")
}

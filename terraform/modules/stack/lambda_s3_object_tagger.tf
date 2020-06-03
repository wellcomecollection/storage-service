module "s3_object_tagger" {
  source = "../lambda"

  name        = "s3_object_tagger-${var.namespace}"
  description = "Add tags to newly-replicated objects (${var.namespace})"
  module_name = "s3_object_tagger"

  s3_bucket = "wellcomecollection-storage-infra"
  s3_key    = "lambdas/s3_object_tagger.zip"

  timeout = 10

  tags = var.tags
}

data "aws_iam_policy_document" "allow_put_object_tag" {
  statement {
    # A PutObjectTag API call completely replaces the tags on an object,
    # so the Lambda has to read the existing tags first, add the new tags,
    # and Put the complete set back.
    #
    # Thus, it needs both Get- and PutObjectTagging.
    actions = [
      "s3:GetObjectTagging",
      "s3:PutObjectTagging",
    ]

    # This reflects the fact that the object tagger's current rules only
    # affect files in the digitised space.  We'll need to change these
    # permissions if we start applying rules to other spaces.
    resources = [
      "arn:aws:s3:::${var.replica_primary_bucket_name}/digitised/*",
    ]
  }
}

resource "aws_iam_role_policy" "allow_s3_object_tagger_to_put_tags" {
  role   = module.s3_object_tagger.role_name
  policy = data.aws_iam_policy_document.allow_put_object_tag.json
}
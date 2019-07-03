module "vhs_manifests" {
  source = "git::https://github.com/wellcometrust/terraform-modules.git//vhs/modules/vhs?ref=4605fbb"
  name   = "${var.namespace}-manifests"
}

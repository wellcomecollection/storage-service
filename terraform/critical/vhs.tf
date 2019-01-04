module "vhs_manifests" {
  source = "git::https://github.com/wellcometrust/terraform-modules.git//vhs/modules/vhs?ref=v18.2.2"
  name   = "${var.namespace}-manifests"
}

module "vhs_manifests" {
  source = "git::https://github.com/wellcometrust/terraform-modules.git//vhs/modules/vhs?ref=v19.2.0"
  name   = "${var.namespace}-manifests"
}

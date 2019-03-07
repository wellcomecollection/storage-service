module "images" {
  source = "git::https://github.com/wellcometrust/terraform.git//ecs/modules/images?ref=v19.8.0"

  label   = "${var.release_label}"
  project = "storage"

  services = [
    "bags_api",
    "ingests",
    "ingests_api",
    "notifier",
    "bagger",
    "bag_register",
    "bag_replicator",
    "bag_verifier",
    "bag_unpacker",
  ]
}

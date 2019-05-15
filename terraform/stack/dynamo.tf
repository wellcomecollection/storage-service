module "auditor_lock_table" {
  source = "../modules/lock_table"

  namespace = "${var.namespace}"
  owner     = "auditor"
}

module "replicator_lock_table" {
  source = "../modules/lock_table"

  namespace = "${var.namespace}"
  owner     = "replicator"
}

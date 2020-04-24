locals {
  bag_replicator_service_name = "${var.namespace}-bag-replicator_${var.replica_id}"
  bag_verifier_service_name   = "${var.namespace}-bag-verifier_${var.replica_id}"
}


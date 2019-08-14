package uk.ac.wellcome.platform.storage.replica_aggregator.models

class ReplicaPath(val underlying: String) extends AnyVal

object ReplicaPath {
  def apply(value: String): ReplicaPath =
    new ReplicaPath(value)
}
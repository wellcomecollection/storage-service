package uk.ac.wellcome.platform.storage.replica_aggregator.models

case class ReplicaPath(val underlying: String) extends AnyVal {
  override def toString: String = underlying
}

object ReplicaPath {
  def apply(value: String): ReplicaPath =
    new ReplicaPath(value)
}

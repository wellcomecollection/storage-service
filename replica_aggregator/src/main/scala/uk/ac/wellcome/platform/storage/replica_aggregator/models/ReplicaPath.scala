package uk.ac.wellcome.platform.storage.replica_aggregator.models

case class ReplicaPath(val underlying: String) extends AnyVal {
  override def toString: String = underlying
}

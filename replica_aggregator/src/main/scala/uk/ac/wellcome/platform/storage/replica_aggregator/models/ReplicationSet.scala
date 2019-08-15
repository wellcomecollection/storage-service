package uk.ac.wellcome.platform.storage.replica_aggregator.models

case class ReplicationSet(
  path: ReplicaPath,
  results: List[ReplicaResult]
)

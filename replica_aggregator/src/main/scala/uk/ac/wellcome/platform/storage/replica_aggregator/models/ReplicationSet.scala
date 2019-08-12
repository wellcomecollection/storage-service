package uk.ac.wellcome.platform.storage.replica_aggregator.models

case class ReplicationSet(
                           replicaIdentifier: ReplicaIdentifier,
                           replicaResult: Set[ReplicaResult]
                         )

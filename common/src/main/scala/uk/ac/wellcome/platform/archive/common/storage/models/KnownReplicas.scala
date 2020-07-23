package uk.ac.wellcome.platform.archive.common.storage.models

/** This records a complete set of replicas which can be passed around
  * between services.
  *
  * This is very similar to AggregatorInternalRecord, but it ensures that
  * we do have a primary replica.
  *
  */
case class KnownReplicas(
  location: PrimaryReplicaLocation,
  replicas: Seq[SecondaryReplicaLocation]
)

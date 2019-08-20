package uk.ac.wellcome.platform.storage.replica_aggregator.models

import uk.ac.wellcome.platform.archive.common.storage.models.{PrimaryStorageLocation, SecondaryStorageLocation}

/** This records a complete set of replicas which can be passed around
  * between services.
  *
  * This is very similar to AggregatorInternalRecord, but it ensures that
  * we do have a primary replica.
  *
  */
case class KnownReplicas(
  location: PrimaryStorageLocation,
  replicas: List[SecondaryStorageLocation]
)

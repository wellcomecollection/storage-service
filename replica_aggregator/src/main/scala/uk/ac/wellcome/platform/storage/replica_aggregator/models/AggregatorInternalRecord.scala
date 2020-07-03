package uk.ac.wellcome.platform.storage.replica_aggregator.models

import uk.ac.wellcome.platform.archive.common.storage.models._

import scala.util.Try

/** This records all the replicas that the aggregator knows about.
  *
  * The location may be None if the aggregator receives notice of
  * a secondary replica first.
  */
case class AggregatorInternalRecord(
  location: Option[PrimaryReplicaLocation],
  replicas: List[SecondaryReplicaLocation]
) {
  def addLocation(location: ReplicaLocation): Try[AggregatorInternalRecord] =
    AggregatorInternalRecord.addLocation(
      record = this,
      replicaLocation = location
    )

  // TODO: Bridging code while we split ObjectLocation.  Remove this later.
  // See https://github.com/wellcomecollection/platform/issues/4596
  def addLocation(location: StorageLocation): Try[AggregatorInternalRecord] =
    AggregatorInternalRecord.addLocation(
      record = this,
      replicaLocation = ReplicaLocation.fromStorageLocation(location)
    )

  def count: Int =
    (Seq(location).flatten ++ replicas).size
}

object AggregatorInternalRecord {
  // TODO: Bridging code while we split ObjectLocation.  Remove this later.
  // See https://github.com/wellcomecollection/platform/issues/4596
  def apply(storageLocation: StorageLocation): AggregatorInternalRecord =
    AggregatorInternalRecord(
      ReplicaLocation.fromStorageLocation(storageLocation)
    )

  def apply(replicaLocation: ReplicaLocation): AggregatorInternalRecord =
    replicaLocation match {
      case primary: PrimaryReplicaLocation =>
        AggregatorInternalRecord(
          location = Some(primary),
          replicas = List.empty
        )
      case secondary: SecondaryReplicaLocation =>
        AggregatorInternalRecord(
          location = None,
          replicas = List(secondary)
        )
    }

  def addLocation(
    record: AggregatorInternalRecord,
    replicaLocation: ReplicaLocation
  ): Try[AggregatorInternalRecord] = {
    replicaLocation match {
      case primaryLocation: PrimaryReplicaLocation =>
        addPrimaryLocation(
          record = record,
          primaryLocation = primaryLocation
        )
      case secondaryLocation: SecondaryReplicaLocation =>
        addSecondaryLocation(
          record = record,
          secondaryLocation = secondaryLocation
        )
    }
  }

  private def addSecondaryLocation(
    record: AggregatorInternalRecord,
    secondaryLocation: SecondaryReplicaLocation
  ) = Try {
    record.copy(
      replicas = (record.replicas.toSet ++ Set(secondaryLocation)).toList
    )
  }

  private def addPrimaryLocation(
    record: AggregatorInternalRecord,
    primaryLocation: PrimaryReplicaLocation
  ): Try[AggregatorInternalRecord] = Try {
    record.location match {
      case None                                          => record.copy(location = Some(primaryLocation))
      case Some(location) if location == primaryLocation => record
      case Some(location) =>
        throw new Exception(
          s"Record already has a different PrimaryStorageLocation: $location (existing) != $primaryLocation"
        )
    }
  }
}

package uk.ac.wellcome.platform.storage.replica_aggregator.models

import uk.ac.wellcome.platform.archive.common.storage.models.{PrimaryStorageLocation, SecondaryStorageLocation, StorageLocation}

import scala.util.Try

/** This records all the replicas that the aggregator knows about.
  *
  * The location may be None if the aggregator receives notice of
  * a secondary replica first.
  */
case class AggregatorInternalRecord(
                                     location: Option[PrimaryStorageLocation],
                                     replicas: List[SecondaryStorageLocation]
                                   ) {
  def addLocation(storageLocation: StorageLocation): Try[AggregatorInternalRecord] =
    AggregatorInternalRecord.addLocation(
      record = this,
      storageLocation = storageLocation
    )
}

object AggregatorInternalRecord {

  def apply(storageLocation: StorageLocation): AggregatorInternalRecord =
    storageLocation match {
      case primary: PrimaryStorageLocation => AggregatorInternalRecord(
        location = Some(primary),
        replicas = List.empty
      )
      case secondary: SecondaryStorageLocation => AggregatorInternalRecord(
        location = None,
        replicas = List(secondary)
      )
    }

  def addLocation(
                   record: AggregatorInternalRecord,
                   storageLocation: StorageLocation
                 ): Try[AggregatorInternalRecord] = {
    storageLocation match {
      case primaryLocation: PrimaryStorageLocation => addPrimaryLocation(
        record = record,
        primaryLocation = primaryLocation
      )
      case secondaryLocation: SecondaryStorageLocation => addSecondaryLocation(
        record = record,
        secondaryLocation = secondaryLocation
      )
    }
  }

  private def addSecondaryLocation(
                                    record: AggregatorInternalRecord,
                                    secondaryLocation: SecondaryStorageLocation
                                  ) = Try {
    record.copy(replicas =
      (record.replicas.toSet ++ Set(secondaryLocation)).toList
    )
  }

  private def addPrimaryLocation(
                                  record: AggregatorInternalRecord,
                                  primaryLocation: PrimaryStorageLocation
                                ): Try[AggregatorInternalRecord] = Try {
    record.location match {
      case None => record.copy(location = Some(primaryLocation))
      case Some(location) if location == primaryLocation => record
      case Some(location) => throw new Exception(
        s"Record already has a different PrimaryStorageLocation: ${location} (existing) != ${primaryLocation}"
      )
    }
  }
}

package uk.ac.wellcome.platform.storage.replica_aggregator.services

import uk.ac.wellcome.platform.archive.common.storage.models.StorageLocation
import uk.ac.wellcome.platform.storage.replica_aggregator.models._
import uk.ac.wellcome.storage.store.VersionedStore
import uk.ac.wellcome.storage.{UpdateError, UpdateNotApplied}

class ReplicaAggregator(
  versionedStore: VersionedStore[ReplicaPath, Int, AggregatorInternalRecord]
) {
  def aggregate(
    storageLocation: StorageLocation
  ): Either[UpdateError, AggregatorInternalRecord] = {
    val replicaPath =
      ReplicaPath(storageLocation.prefix.path)

    val initialRecord = AggregatorInternalRecord(storageLocation)

    val upsert = versionedStore.upsert(replicaPath)(initialRecord) {
      _.addLocation(storageLocation).toEither.left.map(UpdateNotApplied)
    }

    upsert.map(_.identifiedT)
  }
}

package uk.ac.wellcome.platform.storage.replica_aggregator.services

import uk.ac.wellcome.platform.archive.common.storage.models.ReplicaResult
import uk.ac.wellcome.platform.storage.replica_aggregator.models._
import uk.ac.wellcome.storage.store.VersionedStore
import uk.ac.wellcome.storage.{UpdateError, UpdateNotApplied}

class ReplicaAggregator(
  versionedStore: VersionedStore[ReplicaPath, Int, AggregatorInternalRecord]
) {
  def aggregate(
    result: ReplicaResult
  ): Either[UpdateError, AggregatorInternalRecord] = {
    val replicaPath =
      ReplicaPath(result.storageLocation.prefix.path)

    val initialRecord =
      AggregatorInternalRecord(result.storageLocation)

    val upsert = versionedStore.upsert(replicaPath)(initialRecord) {
      _.addLocation(result.storageLocation).toEither.left.map(UpdateNotApplied)
    }

    upsert.map(_.identifiedT)
  }
}

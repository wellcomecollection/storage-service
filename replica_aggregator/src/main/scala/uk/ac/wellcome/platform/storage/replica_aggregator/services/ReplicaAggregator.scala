package uk.ac.wellcome.platform.storage.replica_aggregator.services

import uk.ac.wellcome.platform.storage.replica_aggregator.models._
import uk.ac.wellcome.storage.{Identified, UpdateError, UpdateNotApplied, Version}
import uk.ac.wellcome.storage.store.VersionedStore

class ReplicaAggregator(
  versionedStore: VersionedStore[ReplicaPath, Int, AggregatorInternalRecord]
) {
  def aggregate(result: ReplicaResult
               ): Either[UpdateError, Identified[Version[ReplicaPath, Int], AggregatorInternalRecord]] = {
    val replicaPath =
      ReplicaPath(result.storageLocation.prefix.path)

    val initialRecord =
      AggregatorInternalRecord(result.storageLocation)

    versionedStore.upsert(replicaPath)(initialRecord) {
      _.addLocation(result.storageLocation)
        .toEither.left.map(UpdateNotApplied)
    }
  }
}

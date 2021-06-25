package weco.storage_service.replica_aggregator.services

import weco.storage_service.storage.models.ReplicaLocation
import weco.storage_service.replica_aggregator.models._
import weco.storage.store.VersionedStore
import weco.storage.{UpdateError, UpdateNotApplied}

class ReplicaAggregator(
  versionedStore: VersionedStore[ReplicaPath, Int, AggregatorInternalRecord]
) {
  def aggregate(
    replicaLocation: ReplicaLocation
  ): Either[UpdateError, AggregatorInternalRecord] = {
    val replicaPath = ReplicaPath(replicaLocation.prefix)

    val initialRecord = AggregatorInternalRecord(replicaLocation)

    val upsert = versionedStore.upsert(replicaPath)(initialRecord) {
      _.addLocation(replicaLocation).toEither.left.map(UpdateNotApplied)
    }

    upsert.map(_.identifiedT)
  }
}

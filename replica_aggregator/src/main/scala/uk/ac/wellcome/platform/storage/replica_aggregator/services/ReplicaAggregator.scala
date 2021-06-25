package uk.ac.wellcome.platform.storage.replica_aggregator.services

import weco.storage_service.storage.models.ReplicaLocation
import uk.ac.wellcome.platform.storage.replica_aggregator.models._
import weco.storage.store.VersionedStore
import uk.ac.wellcome.storage.{UpdateError, UpdateNotApplied}

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

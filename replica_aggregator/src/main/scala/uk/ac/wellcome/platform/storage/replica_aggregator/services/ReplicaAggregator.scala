package uk.ac.wellcome.platform.storage.replica_aggregator.services

import uk.ac.wellcome.platform.storage.replica_aggregator.models._
import uk.ac.wellcome.storage.Identified
import uk.ac.wellcome.storage.store.VersionedStore

import scala.util.{Failure, Success, Try}

class ReplicaAggregator(
  versionedStore: VersionedStore[ReplicaPath, Int, AggregatorInternalRecord]
) {
  def aggregate(result: ReplicaResult): Try[AggregatorInternalRecord] = {
    val replicaPath =
      ReplicaPath(result.storageLocation.prefix.path)

    val initialRecord =
      AggregatorInternalRecord(result.storageLocation)

    versionedStore.upsert(replicaPath)(initialRecord) { existingRecord =>
      // TODO: This .get is caused by poor handling of error states in the storage library.
      // See: https://github.com/wellcometrust/platform/issues/3840

      existingRecord.addLocation(result.storageLocation).get
    } match {
      case Right(Identified(_, aggregatorRecord)) =>
        Success(aggregatorRecord)

      case Left(updateError) =>
        Failure(updateError.e)
    }
  }
}

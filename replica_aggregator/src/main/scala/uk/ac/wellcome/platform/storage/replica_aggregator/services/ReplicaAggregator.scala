package uk.ac.wellcome.platform.storage.replica_aggregator.services

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.storage.models.SecondaryStorageLocation
import uk.ac.wellcome.platform.storage.replica_aggregator.models._
import uk.ac.wellcome.storage.store.VersionedStore

import scala.util.Try

class ReplicaAggregator(
  versionedStore: VersionedStore[ReplicaPath, Int, AggregatorInternalRecord]
) {
  def aggregate(result: ReplicaResult): Try[ReplicationAggregationSummary] =
    Try {

      // When we add support for secondary storage locations, we need to tweak
      // the logic for deciding if a replica is "complete" -- we'll need to
      // check we have:
      //
      //    * at least one primary replica
      //    * at least N secondary replicas, where N is configurable
      //
      // For the initial, simpler case, we just handle primary replicas, so
      // error out if passed a secondary replica.
      //
      result.storageLocation match {
        case _: SecondaryStorageLocation =>
          throw new Throwable(
            s"Not yet supported! Cannot aggregate secondary replica result: $result"
          )

        case _ => ()
      }

      val startTime = Instant.now()

      val replicaPath =
        ReplicaPath(result.storageLocation.prefix.path)

      val initialRecord =
        AggregatorInternalRecord(result.storageLocation)

      versionedStore.upsert(replicaPath)(initialRecord) { existingRecord =>
        // TODO: This .get is caused by poor handling of error states in the storage library.
        // See: https://github.com/wellcometrust/platform/issues/3840

        existingRecord.addLocation(result.storageLocation).get
      } match {
        // Only a single result is enough for now.
        case Right(upsertResult) =>

          // TODO: Remove
          val aggregatorRecord = upsertResult.identifiedT
          val results =
            (Seq(aggregatorRecord.location).flatten ++ aggregatorRecord.replicas)
              .map { loc =>
                ReplicaResult(
                  ingestId = result.ingestId,
                  storageLocation = loc,
                  timestamp = result.timestamp
                )
              }
              .toList

          val replicationSet = ReplicationSet(
            path = replicaPath,
            results = results
          )

          ReplicationAggregationComplete(
            replicationSet = replicationSet,
            startTime = startTime,
            endTime = Instant.now()
          )

        case Left(updateError) =>
          ReplicationAggregationFailed(
            e = updateError.e,
            replicaPath = replicaPath,
            startTime = startTime,
            endTime = Instant.now()
          )
      }
    }
}

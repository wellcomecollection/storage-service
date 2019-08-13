package uk.ac.wellcome.platform.storage.replica_aggregator.services

import java.time.Instant

import uk.ac.wellcome.platform.storage.replica_aggregator.models._
import uk.ac.wellcome.storage.store.VersionedStore
import uk.ac.wellcome.storage.{Identified, Version}

import scala.util.Try

class ReplicaAggregator(
  versionedStore: VersionedStore[String, Int, Set[ReplicaResult]]
) {
  def aggregate(result: ReplicaResult): Try[ReplicationAggregationSummary] = Try {

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
        throw new Throwable(s"Not yet supported! Cannot aggregate secondary replica result: $result")

      case _ => ()
    }

    val startTime = Instant.now()

    val replicaPath = result.storageLocation.location.path

    versionedStore.upsert(replicaPath)(Set(result)) { existing =>
      existing ++ Set(result)
    } match {
      // Only a single result is enough for now.
      case Right(upsertResult: Identified[Version[String, Int], Set[ReplicaResult]]) =>
        val replicationSet = ReplicationSet(
          path = replicaPath,
          results = upsertResult.identifiedT
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

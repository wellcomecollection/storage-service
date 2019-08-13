package uk.ac.wellcome.platform.storage.replica_aggregator.services

import java.time.Instant

import uk.ac.wellcome.platform.storage.replica_aggregator.models._
import uk.ac.wellcome.storage.{Identified, Version}
import uk.ac.wellcome.storage.store.VersionedStore

import scala.util.{Success, Try}

class ReplicaAggregator(
  versionedStore: VersionedStore[String, Int, Set[ReplicaResult]]
) {
  def aggregate(result: ReplicaResult): Try[ReplicationAggregationSummary] = {
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

        Success(
          ReplicationAggregationComplete(
            replicationSet = replicationSet,
            startTime = startTime,
            endTime = Instant.now()
          )
        )

      case Left(updateError) =>
        throw new Throwable("BOOM!")
    }
  }
}

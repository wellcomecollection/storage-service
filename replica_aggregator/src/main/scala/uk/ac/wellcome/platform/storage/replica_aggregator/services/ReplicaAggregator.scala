package uk.ac.wellcome.platform.storage.replica_aggregator.services

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestFailed,
  IngestStepResult,
  IngestStepSucceeded
}
import uk.ac.wellcome.platform.storage.replica_aggregator.models._
import uk.ac.wellcome.storage.UpdateError
import uk.ac.wellcome.storage.store.VersionedStore

import scala.util.{Success, Try}

class ReplicaAggregator(
  versionedStore: VersionedStore[ReplicaIdentifier, Int, Set[ReplicaResult]]
) {

  def aggregate(
    replicaIdentifier: ReplicaIdentifier,
    replicaResult: ReplicaResult
  ): Try[IngestStepResult[ReplicationAggregationSummary]] = {
    val startTime = Instant.now

    // One location is enough for now, we always complete successfully
    val aggregation: Either[UpdateError, IngestStepSucceeded[
      ReplicationAggregationComplete
    ]] = for {
      upsert <- versionedStore.upsert(replicaIdentifier)(Set(replicaResult))(
        _ + replicaResult
      )
    } yield IngestStepSucceeded(
      summary = ReplicationAggregationComplete(
        replicationSet = ReplicationSet(replicaIdentifier, upsert.identifiedT),
        startTime = startTime,
        endTime = Instant.now
      )
    )

    aggregation match {
      case Left(failed) =>
        Success(
          IngestFailed(
            ReplicationAggregationFailed(
              replicationSet = ReplicationSet(replicaIdentifier, Set.empty),
              startTime = startTime,
              endTime = Instant.now
            ),
            failed.e,
            Some("Failed to aggregate replicas!")
          )
        )
      case Right(succeeded) => Success(succeeded)
    }
  }
}

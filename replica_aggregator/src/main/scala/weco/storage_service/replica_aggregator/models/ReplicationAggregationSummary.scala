package weco.storage_service.replica_aggregator.models

import java.time.Instant

import weco.storage_service.ingests.models.IngestID
import weco.storage_service.operation.models.Summary
import weco.storage_service.storage.models.KnownReplicas
import weco.storage_service.replica_aggregator.services.ReplicaCounterError

sealed trait ReplicationAggregationSummary extends Summary {
  val replicaPath: ReplicaPath
  val endTime: Instant

  override val maybeEndTime: Option[Instant] = Some(endTime)

  override val fieldsToLog: Seq[(String, Any)] = {
    val status = this match {
      case _: ReplicationAggregationComplete =>
        "complete"
      case _: ReplicationAggregationIncomplete =>
        "incomplete"
      case _: ReplicationAggregationFailed =>
        "failed"
    }

    Seq(
      ("replicaPath", replicaPath),
      ("status", status)
    )
  }
}

case class ReplicationAggregationComplete(
  ingestId: IngestID,
  replicaPath: ReplicaPath,
  knownReplicas: KnownReplicas,
  startTime: Instant,
  endTime: Instant
) extends ReplicationAggregationSummary

case class ReplicationAggregationIncomplete(
  ingestId: IngestID,
  replicaPath: ReplicaPath,
  aggregatorRecord: AggregatorInternalRecord,
  counterError: ReplicaCounterError,
  startTime: Instant,
  endTime: Instant
) extends ReplicationAggregationSummary

case class ReplicationAggregationFailed(
  ingestId: IngestID,
  e: Throwable,
  replicaPath: ReplicaPath,
  startTime: Instant,
  endTime: Instant
) extends ReplicationAggregationSummary

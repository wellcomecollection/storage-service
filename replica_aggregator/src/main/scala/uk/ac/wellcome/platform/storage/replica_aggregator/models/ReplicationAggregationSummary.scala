package uk.ac.wellcome.platform.storage.replica_aggregator.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.operation.models.Summary

sealed trait ReplicationAggregationSummary extends Summary {
  val replicaPath: ReplicaPath
  val endTime: Instant

  override val maybeEndTime: Option[Instant] = Some(endTime)

  override def toString: String = {
    val status = this match {
      case _: ReplicationAggregationComplete =>
        """
          |status=complete
         """.stripMargin
      case _: ReplicationAggregationIncomplete =>
        """
          |status=incomplete
        """.stripMargin
      case _: ReplicationAggregationFailed =>
        """
          |status=failed
        """.stripMargin
    }

    f"""|replicaPath=$replicaPath
        |durationSeconds=$durationSeconds
        |duration=$formatDuration
        |$status
      """.stripMargin
      .replaceAll("\n", ", ")
  }
}

case class ReplicationAggregationComplete(
  replicationSet: ReplicationSet,
  startTime: Instant,
  endTime: Instant
) extends ReplicationAggregationSummary {
  val replicaPath: ReplicaPath = replicationSet.path
}

case class ReplicationAggregationIncomplete(
  replicationSet: ReplicationSet,
  startTime: Instant,
  endTime: Instant
) extends ReplicationAggregationSummary {
  val replicaPath: ReplicaPath = replicationSet.path
}

case class ReplicationAggregationFailed(
  e: Throwable,
  replicaPath: ReplicaPath,
  startTime: Instant,
  endTime: Instant
) extends ReplicationAggregationSummary

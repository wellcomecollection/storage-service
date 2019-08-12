package uk.ac.wellcome.platform.storage.replica_aggregator.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.operation.models.Summary

sealed trait ReplicationAggregationSummary extends Summary {
  val replicationSet: ReplicationSet
  val endTime: Instant

  override val maybeEndTime: Option[Instant] = Some(endTime)

  override def toString: String = {

    val status = this match {
      case _: ReplicationAggregationComplete =>
        f"""
           |status=complete
         """.stripMargin
      case _: ReplicationAggregationIncomplete =>
        f"""
           |status=incomplete
        """.stripMargin

        f"""|externalIdentifier=${replicationSet.replicaIdentifier.externalIdentifier}
            |durationSeconds=$durationSeconds
            |duration=$formatDuration
            |$status
          """.stripMargin
          .
            replaceAll("\n", ", ")
    }
  }
}


case class ReplicationAggregationComplete(
                                           replicationSet: ReplicationSet,
                                           startTime: Instant,
                                           endTime: Instant
                                         ) extends ReplicationAggregationSummary

case class ReplicationAggregationIncomplete(
                                             replicationSet: ReplicationSet,
                                             startTime: Instant,
                                             endTime: Instant
                                           ) extends ReplicationAggregationSummary

case class ReplicationAggregationFailed(
                                             replicationSet: ReplicationSet,
                                             startTime: Instant,
                                             endTime: Instant
                                           ) extends ReplicationAggregationSummary

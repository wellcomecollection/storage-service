package uk.ac.wellcome.platform.archive.bagreplicator.replicator.models

sealed trait ReplicationResult {
  val summary: ReplicationSummary
}

case class ReplicationSucceeded(summary: ReplicationSummary)
    extends ReplicationResult

case class ReplicationFailed(summary: ReplicationSummary, e: Throwable)
    extends ReplicationResult

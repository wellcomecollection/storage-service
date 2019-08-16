package uk.ac.wellcome.platform.archive.bagreplicator.bags.models

sealed trait BagReplicationResult {
  val summary: BagReplicationSummary
}

case class BagReplicationSucceeded(summary: BagReplicationSummary)
  extends BagReplicationResult

case class BagReplicationFailed(summary: BagReplicationSummary, e: Throwable)
  extends BagReplicationResult

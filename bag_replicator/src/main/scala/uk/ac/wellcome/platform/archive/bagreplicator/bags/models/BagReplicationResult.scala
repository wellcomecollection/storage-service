package uk.ac.wellcome.platform.archive.bagreplicator.bags.models

sealed trait BagReplicationResult[Request <: BagReplicationRequest] {
  val summary: BagReplicationSummary[Request]
}

case class BagReplicationSucceeded[Request <: BagReplicationRequest](
  summary: BagReplicationSummary[Request]
) extends BagReplicationResult[Request]

case class BagReplicationFailed[Request <: BagReplicationRequest](
  summary: BagReplicationSummary[Request],
  e: Throwable
) extends BagReplicationResult[Request]

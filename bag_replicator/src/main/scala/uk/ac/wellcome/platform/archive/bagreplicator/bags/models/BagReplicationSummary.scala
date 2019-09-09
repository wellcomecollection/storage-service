package uk.ac.wellcome.platform.archive.bagreplicator.bags.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.storage.ObjectLocationPrefix

case class BagReplicationSummary[Request <: BagReplicationRequest](
  ingestId: IngestID,
  startTime: Instant,
  maybeEndTime: Option[Instant] = None,
  request: Request
) extends Summary {

  def srcPrefix: ObjectLocationPrefix = request.request.srcPrefix
  def dstPrefix: ObjectLocationPrefix = request.request.dstPrefix

  def complete: BagReplicationSummary[Request] = this.copy(
    maybeEndTime = Some(Instant.now)
  )

  override val fieldsToLog: Seq[(String, Any)] =
    Seq(("request", request))
}

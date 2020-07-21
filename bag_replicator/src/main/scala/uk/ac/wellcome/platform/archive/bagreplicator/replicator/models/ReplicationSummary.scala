package uk.ac.wellcome.platform.archive.bagreplicator.replicator.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.operation.models.Summary

case class ReplicationSummary(
  ingestId: IngestID,
  startTime: Instant,
  maybeEndTime: Option[Instant] = None,
  request: ReplicationRequest
) extends Summary {
  def complete: ReplicationSummary = this.copy(
    maybeEndTime = Some(Instant.now)
  )

  override val fieldsToLog: Seq[(String, Any)] =
    Seq(("request", request))
}

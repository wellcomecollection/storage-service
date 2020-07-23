package uk.ac.wellcome.platform.archive.bagreplicator.replicator.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.storage.{Location, Prefix}

case class ReplicationSummary[DstPrefix <: Prefix[_ <: Location]](
  ingestId: IngestID,
  startTime: Instant,
  maybeEndTime: Option[Instant] = None,
  request: ReplicationRequest[DstPrefix]
) extends Summary {
  def complete: ReplicationSummary[DstPrefix] = this.copy(
    maybeEndTime = Some(Instant.now)
  )

  override val fieldsToLog: Seq[(String, Any)] =
    Seq(("request", request))
}

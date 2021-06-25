package weco.storage_service.bag_replicator.replicator.models

import java.time.Instant

import weco.storage_service.ingests.models.IngestID
import weco.storage_service.operation.models.Summary
import weco.storage.{Location, Prefix}

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

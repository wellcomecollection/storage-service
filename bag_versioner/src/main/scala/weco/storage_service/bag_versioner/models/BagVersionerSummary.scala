package weco.storage_service.bag_versioner.models

import java.time.Instant

import weco.storage_service.bagit.models.BagVersion
import weco.storage_service.ingests.models.IngestID
import weco.storage_service.operation.models.Summary

sealed trait BagVersionerSummary extends Summary {
  val endTime: Instant
  override val maybeEndTime: Option[Instant] = Some(endTime)
}

case class BagVersionerFailureSummary(
  ingestId: IngestID,
  startTime: Instant,
  endTime: Instant
) extends BagVersionerSummary {
  override val fieldsToLog: Seq[(String, Any)] = Seq.empty
}

case class BagVersionerSuccessSummary(
  ingestId: IngestID,
  startTime: Instant,
  endTime: Instant,
  version: BagVersion
) extends BagVersionerSummary {
  override val fieldsToLog: Seq[(String, Any)] = Seq(("version", version))
}

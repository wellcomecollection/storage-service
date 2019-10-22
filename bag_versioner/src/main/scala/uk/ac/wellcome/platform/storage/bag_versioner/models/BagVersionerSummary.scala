package uk.ac.wellcome.platform.storage.bag_versioner.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.operation.models.Summary

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

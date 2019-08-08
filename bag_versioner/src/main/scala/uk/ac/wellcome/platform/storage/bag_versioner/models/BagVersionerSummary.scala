package uk.ac.wellcome.platform.storage.bag_versioner.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
import uk.ac.wellcome.platform.archive.common.operation.models.Summary

sealed trait BagVersionerSummary extends Summary {
  val endTime: Instant
  override val maybeEndTime: Option[Instant] = Some(endTime)
}

case class BagVersionerFailureSummary(
  startTime: Instant,
  endTime: Instant
) extends BagVersionerSummary

case class BagVersionerSuccessSummary(
  startTime: Instant,
  endTime: Instant,
  version: BagVersion
) extends BagVersionerSummary

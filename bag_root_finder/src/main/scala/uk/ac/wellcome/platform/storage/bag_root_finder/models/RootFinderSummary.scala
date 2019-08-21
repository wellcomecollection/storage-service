package uk.ac.wellcome.platform.storage.bag_root_finder.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.storage.ObjectLocationPrefix

sealed trait RootFinderSummary extends Summary {
  val location: ObjectLocationPrefix

  val endTime: Instant
  override val maybeEndTime: Option[Instant] = Some(endTime)
}

case class RootFinderFailureSummary(
  startTime: Instant,
  endTime: Instant,
  location: ObjectLocationPrefix
) extends RootFinderSummary

case class RootFinderSuccessSummary(
  startTime: Instant,
  endTime: Instant,
  location: ObjectLocationPrefix,
  rootLocation: ObjectLocationPrefix
) extends RootFinderSummary

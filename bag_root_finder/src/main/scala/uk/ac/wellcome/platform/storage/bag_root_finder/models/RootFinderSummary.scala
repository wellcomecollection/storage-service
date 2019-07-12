package uk.ac.wellcome.platform.storage.bag_root_finder.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

sealed trait RootFinderSummary extends Summary {
  val location: ObjectLocationPrefix
}

case class RootFinderFailureSummary(
  location: ObjectLocationPrefix,
  startTime: Instant,
  endTime: Option[Instant]
) extends RootFinderSummary

case class RootFinderSuccessSummary(
  location: ObjectLocationPrefix,
  startTime: Instant,
  bagRootLocation: ObjectLocation,
  endTime: Option[Instant]
) extends RootFinderSummary

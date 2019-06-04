package uk.ac.wellcome.platform.storage.bag_root_finder.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation

sealed trait RootFinderSummary extends Summary {
  val location: ObjectLocation
}

case class RootFinderFailureSummary(
  location: ObjectLocation,
  space: StorageSpace,
  startTime: Instant,
  endTime: Option[Instant]
) extends RootFinderSummary

case class RootFinderSuccessSummary(
  location: ObjectLocation,
  space: StorageSpace,
  startTime: Instant,
  bagRootLocation: ObjectLocation,
  endTime: Option[Instant]
) extends RootFinderSummary

package uk.ac.wellcome.platform.archive.bag_tracker.client

sealed trait BagTrackerError

sealed trait BagTrackerListVersionsError extends BagTrackerError
case class BagTrackerNotFoundListError() extends BagTrackerListVersionsError
case class BagTrackerUnknownListError(err: Throwable) extends BagTrackerListVersionsError


package uk.ac.wellcome.platform.archive.bag_tracker.client

sealed trait BagTrackerError

sealed trait BagTrackerGetError extends BagTrackerError

case class BagTrackerUnknownGetError(err: Throwable) extends BagTrackerGetError

sealed trait BagTrackerListVersionsError extends BagTrackerError
case class BagTrackerUnknownListError(err: Throwable)
    extends BagTrackerListVersionsError

case class BagTrackerNotFoundError()
    extends BagTrackerListVersionsError
    with BagTrackerGetError

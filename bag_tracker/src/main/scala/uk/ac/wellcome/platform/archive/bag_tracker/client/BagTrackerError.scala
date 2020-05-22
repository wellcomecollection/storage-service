package uk.ac.wellcome.platform.archive.bag_tracker.client

import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, BagVersion}

sealed trait BagTrackerError

sealed trait BagTrackerListVersionsError extends BagTrackerError

case class BagTrackerNotFoundListError(bagId: BagId, maybeBefore: Option[BagVersion])
  extends BagTrackerListVersionsError

case class BagTrackerUnknownListError(bagId: BagId, maybeBefore: Option[BagVersion], err: Throwable)
  extends BagTrackerListVersionsError


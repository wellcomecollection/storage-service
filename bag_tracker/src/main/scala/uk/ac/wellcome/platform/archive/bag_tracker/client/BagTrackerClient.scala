package uk.ac.wellcome.platform.archive.bag_tracker.client

import uk.ac.wellcome.platform.archive.bag_tracker.models.BagVersionList
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, BagVersion}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest

import scala.concurrent.Future

trait BagTrackerClient {
  def createBag(
    storageManifest: StorageManifest
  ): Future[Either[BagTrackerError, Unit]]

  def getLatestBag(
    bagId: BagId
  ): Future[Either[BagTrackerError, StorageManifest]]

  def getBag(
    bagId: BagId,
    version: BagVersion
  ): Future[Either[BagTrackerError, StorageManifest]]

  def getVersionsOf(
    bagId: BagId,
    before: Option[BagVersion]
  ): Future[Either[BagTrackerError, BagVersionList]]
}

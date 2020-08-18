package uk.ac.wellcome.platform.archive.bag_tracker.services

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bag_tracker.storage.StorageManifestDao
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.storage.NoVersionExistsError

trait GetLatestBag extends Logging {
  val storageManifestDao: StorageManifestDao

  def getLatestBag(bagId: BagId): Route = {
    storageManifestDao.getLatest(bagId) match {
      case Right(manifest) =>
        info(s"Found latest version of bag $bagId is ${manifest.version}")
        complete(manifest)

      case Left(_: NoVersionExistsError) =>
        info(s"Could not find any versions of bag $bagId")
        complete(StatusCodes.NotFound)

      case Left(err) =>
        warn(s"Unexpected error looking for latest version of bag $bagId: $err")
        complete(StatusCodes.InternalServerError)
    }
  }
}

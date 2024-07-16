package weco.storage_service.bag_tracker.services

import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Directives.complete
import org.apache.pekko.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import grizzled.slf4j.Logging
import weco.json.JsonUtil._
import weco.storage_service.bag_tracker.storage.StorageManifestDao
import weco.storage_service.bagit.models.BagId
import weco.storage.NoVersionExistsError

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

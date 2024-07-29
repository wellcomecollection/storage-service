package weco.storage_service.bag_tracker.services

import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Directives.complete
import org.apache.pekko.http.scaladsl.server.Route
import grizzled.slf4j.Logging
import weco.storage_service.bag_tracker.storage.StorageManifestDao
import weco.storage_service.storage.models.StorageManifest

trait CreateBag extends Logging {
  val storageManifestDao: StorageManifestDao

  def createBag(storageManifest: StorageManifest): Route =
    storageManifestDao.put(storageManifest) match {
      case Right(_) => complete(StatusCodes.Created)

      case Left(storageError) =>
        warn(
          s"Unexpected error storing bag ${storageManifest.idWithVersion}",
          storageError.e
        )
        complete(StatusCodes.InternalServerError)
    }
}

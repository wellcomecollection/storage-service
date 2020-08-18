package uk.ac.wellcome.platform.archive.bag_tracker.services

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.Route
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bag_tracker.storage.StorageManifestDao
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest

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

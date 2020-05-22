package uk.ac.wellcome.platform.archive.bag_tracker.services

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, BagVersion}
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao
import uk.ac.wellcome.storage.{DoesNotExistError, NoVersionExistsError}

trait GetBag extends Logging {
  val storageManifestDao: StorageManifestDao

  def getBag(bagId: BagId, version: BagVersion): Route = {
    storageManifestDao.get(id = bagId, version = version) match {
      case Right(manifest) =>
        info(s"Found bag id=$bagId version=$version")
        complete(manifest)

      case Left(_: DoesNotExistError) | Left(_: NoVersionExistsError) =>
        info(s"Could not find bag id=$bagId version=$version")
        complete(StatusCodes.NotFound)

      case Left(err) =>
        warn(s"Unexpected error looking for bag id=$bagId version=$version: $err")
        complete(StatusCodes.InternalServerError)
    }
  }
}

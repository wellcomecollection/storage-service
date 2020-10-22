package uk.ac.wellcome.platform.archive.bag_tracker.services

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bag_tracker.storage.StorageManifestDao
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, BagVersion}
import uk.ac.wellcome.storage.{
  DoesNotExistError,
  NoVersionExistsError,
  ReadError
}

trait GetBag extends Logging {
  val storageManifestDao: StorageManifestDao

  protected def handleGetErrors(
    bagId: BagId,
    version: BagVersion,
    readError: ReadError
  ): Route =
    readError match {
      case _: DoesNotExistError | _: NoVersionExistsError =>
        info(s"Could not find bag id=$bagId version=$version")
        complete(StatusCodes.NotFound)

      case err =>
        warn(
          s"Unexpected error looking for bag id=$bagId version=$version: $err"
        )
        complete(StatusCodes.InternalServerError)
    }

  def getBag(bagId: BagId, version: BagVersion): Route = {
    storageManifestDao.get(id = bagId, version = version) match {
      case Right(manifest) =>
        info(s"Found bag id=$bagId version=$version")
        complete(manifest)

      case Left(err) => handleGetErrors(bagId, version, err)
    }
  }
}

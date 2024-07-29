package weco.storage_service.bag_tracker.services

import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Directives.complete
import org.apache.pekko.http.scaladsl.server.Route
import com.github.pjfanning.pekkohttpcirce.ErrorAccumulatingCirceSupport._
import grizzled.slf4j.Logging
import weco.json.JsonUtil._
import weco.storage_service.bag_tracker.storage.StorageManifestDao
import weco.storage_service.bagit.models.{BagId, BagVersion}
import weco.storage.{DoesNotExistError, NoVersionExistsError}

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
        warn(
          s"Unexpected error looking for bag id=$bagId version=$version: $err"
        )
        complete(StatusCodes.InternalServerError)
    }
  }
}

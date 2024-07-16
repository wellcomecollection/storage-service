package weco.storage_service.bag_tracker.services

import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Directives.complete
import org.apache.pekko.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import grizzled.slf4j.Logging
import weco.json.JsonUtil._
import weco.storage_service.bag_tracker.models.{BagVersionEntry, BagVersionList}
import weco.storage_service.bag_tracker.storage.StorageManifestDao
import weco.storage_service.bagit.models.{BagId, BagVersion}

import scala.concurrent.ExecutionContext

trait LookupBagVersions extends Logging {
  val storageManifestDao: StorageManifestDao

  implicit val ec: ExecutionContext

  def lookupVersions(bagId: BagId, maybeBefore: Option[BagVersion]): Route =
    storageManifestDao.listVersions(bagId, before = maybeBefore) match {
      case Right(Nil) =>
        info(
          s"Could not find any versions for $bagId that were before $maybeBefore"
        )
        complete(StatusCodes.NotFound)

      case Right(storageManifests) =>
        info(
          s"Found ${storageManifests.size} version(s) for $bagId that were before $maybeBefore"
        )
        val bagVersionList = BagVersionList(
          id = bagId,
          versions = storageManifests.map { manifest =>
            BagVersionEntry(
              version = manifest.version,
              createdDate = manifest.createdDate
            )
          }
        )

        complete(bagVersionList)

      case Left(err) =>
        warn(
          s"Unexpected error looking for versions of $bagId that were before $maybeBefore: $err"
        )
        complete(StatusCodes.InternalServerError)
    }
}

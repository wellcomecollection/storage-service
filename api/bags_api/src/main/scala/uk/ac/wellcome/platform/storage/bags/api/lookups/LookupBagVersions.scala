package uk.ac.wellcome.platform.storage.bags.api.lookups

import java.net.URL

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.StandardRoute
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.http.models.{
  InternalServerErrorResponse,
  UserErrorResponse
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao
import uk.ac.wellcome.platform.storage.bags.api.models.{
  DisplayResultList,
  ResultListEntry
}
import uk.ac.wellcome.storage.ReadError

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

trait LookupBagVersions extends Logging with LookupBase {
  val contextURL: URL
  val storageManifestDao: StorageManifestDao

  implicit val ec: ExecutionContext

  def lookupVersions(bagId: BagId, maybeBefore: Option[String]): StandardRoute =
    parseVersion(maybeBefore) match {
      case Success(Some(version)) =>
        buildResultsList(
          bagId = bagId,
          storageManifestDao.listVersions(bagId, before = version),
          notFoundMessage =
            s"No storage manifest versions found for $bagId before $version"
        )

      case Success(None) =>
        buildResultsList(
          bagId = bagId,
          storageManifestDao.listVersions(bagId),
          notFoundMessage = s"No storage manifest versions found for $bagId"
        )

      // Note: if the version is empty, we'll always be able to parse it,
      // so the .get here is safe.
      case Failure(_) =>
        complete(
          BadRequest -> UserErrorResponse(
            context = contextURL,
            statusCode = StatusCodes.BadRequest,
            description = s"Cannot parse version string: ${maybeBefore.get}"
          )
        )
    }

  private def buildResultsList(
    bagId: BagId,
    matchingManifests: Either[ReadError, Seq[StorageManifest]],
    notFoundMessage: String
  ): StandardRoute =
    matchingManifests match {
      case Right(Nil) =>
        complete(
          NotFound -> UserErrorResponse(
            context = contextURL,
            statusCode = StatusCodes.NotFound,
            description = notFoundMessage
          )
        )

      case Right(manifests) =>
        complete(
          DisplayResultList(
            context = contextURL.toString,
            results = manifests.map {
              ResultListEntry(_)
            }
          )
        )

      case Left(err) =>
        error(s"Error while trying to look up versions of $bagId", err.e)
        complete(
          InternalServerError -> InternalServerErrorResponse(
            context = contextURL,
            statusCode = StatusCodes.InternalServerError
          )
        )
    }
}

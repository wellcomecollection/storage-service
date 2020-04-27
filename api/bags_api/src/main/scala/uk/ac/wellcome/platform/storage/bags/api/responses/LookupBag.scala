package uk.ac.wellcome.platform.storage.bags.api.responses

import java.net.URL

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.ETag
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, BagVersion}
import uk.ac.wellcome.platform.archive.common.config.models.HTTPServerConfig
import uk.ac.wellcome.platform.archive.common.http.models.{
  InternalServerErrorResponse,
  UserErrorResponse
}
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao
import uk.ac.wellcome.platform.archive.display.manifests.DisplayStorageManifest
import uk.ac.wellcome.storage.{NoMaximaValueError, NoVersionExistsError}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

trait LookupBag extends Logging with ResponseBase {
  val httpServerConfig: HTTPServerConfig
  val contextURL: URL

  val storageManifestDao: StorageManifestDao

  implicit val ec: ExecutionContext

  def lookupBag(bagId: BagId, versionString: String): Route = {
    val result = parseVersion(versionString) match {
      case Success(version) =>
        storageManifestDao.get(bagId, version = version)
      case Failure(_) => Left(NoVersionExistsError())
    }

    val etag = createEtag(bagId = bagId, versionString = versionString)

    result match {
      case Right(storageManifest) =>
        respondWithHeaders(etag) {
          complete(
            DisplayStorageManifest(
              storageManifest = storageManifest,
              contextUrl = contextURL
            )
          )
        }

      case Left(_: NoVersionExistsError) =>
        complete(
          NotFound -> UserErrorResponse(
            context = contextURL,
            statusCode = StatusCodes.NotFound,
            description = s"Storage manifest $bagId $versionString not found"
          )
        )

      case Left(storageError) =>
        error(
          s"Error while trying to look up bagId=$bagId version=$versionString",
          storageError.e
        )
        complete(
          InternalServerError -> InternalServerErrorResponse(
            context = contextURL,
            statusCode = StatusCodes.InternalServerError
          )
        )
    }
  }

  // Either returns the latest version, or a response to send to the user explaining
  // why we couldn't find the latest version.
  protected def getLatestVersion(bagId: BagId): Either[Route, BagVersion] =
    storageManifestDao.getLatestVersion(bagId) match {
      case Right(version) => Right(version)

      case Left(_: NoMaximaValueError) =>
        Left(
          complete(
            NotFound -> UserErrorResponse(
              context = contextURL,
              statusCode = StatusCodes.NotFound,
              description = s"Storage manifest $bagId not found"
            )
          )
        )

      case Left(readError) =>
        error(
          s"Error while trying to find the latest version of $bagId",
          readError.e
        )
        Left(
          complete(
            InternalServerError -> InternalServerErrorResponse(
              context = contextURL,
              statusCode = StatusCodes.InternalServerError
            )
          )
        )
    }

  def createEtag(bagId: BagId, versionString: String): ETag =
    ETag(s"$bagId/$versionString")
}

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
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.http.models.{
  InternalServerErrorResponse,
  UserErrorResponse
}
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao
import uk.ac.wellcome.platform.storage.bags.api.models.ResponseDisplayBag
import uk.ac.wellcome.storage.NoVersionExistsError

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

trait LookupBag extends Logging with ResponseBase {
  val contextURL: URL
  val storageManifestDao: StorageManifestDao

  implicit val ec: ExecutionContext

  def lookupBag(bagId: BagId, maybeVersion: Option[String]): Route = {
    val result = maybeVersion match {
      case None => storageManifestDao.getLatest(bagId)

      case Some(versionString) =>
        parseVersion(versionString) match {
          case Success(version) => storageManifestDao.get(bagId, version = version)
          case Failure(_)       => Left(NoVersionExistsError())
        }
    }

    result match {
      case Right(storageManifest) =>
        val etag = ETag(storageManifest.idWithVersion)

        respondWithHeaders(etag) {
          complete(
            ResponseDisplayBag(
              storageManifest = storageManifest,
              contextUrl = contextURL
            )
          )
        }

      case Left(_: NoVersionExistsError) =>
        val errorMessage = maybeVersion match {
          case Some(version) =>
            s"Storage manifest $bagId version $version not found"
          case None => s"Storage manifest $bagId not found"
        }

        complete(
          NotFound -> UserErrorResponse(
            context = contextURL,
            statusCode = StatusCodes.NotFound,
            description = errorMessage
          )
        )
      case Left(storageError) =>
        error(
          s"Error while trying to look up $bagId v = $maybeVersion",
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
}

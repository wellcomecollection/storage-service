package uk.ac.wellcome.platform.storage.bags.api.responses

import java.net.URL

import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.Uri.Query
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
import uk.ac.wellcome.platform.storage.bags.api.models.ResponseDisplayBag
import uk.ac.wellcome.storage.{NoMaximaValueError, NoVersionExistsError}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

trait LookupBag extends Logging with ResponseBase {
  val httpServerConfig: HTTPServerConfig
  val contextURL: URL

  val storageManifestDao: StorageManifestDao

  implicit val ec: ExecutionContext

  def createLookupBagUrl(bagId: BagId, version: BagVersion): Uri = {
    val baseUri = Uri(httpServerConfig.externalBaseURL.toString)

    val newPath = baseUri.path / "bags" / bagId.space.toString / bagId.externalIdentifier.toString

    val newParams: Map[String, String] = baseUri.query().toMap ++ Map("version" -> version.toString)
    val newQuery: Query = Query(newParams)

    baseUri
      .withPath(newPath)
      .withQuery(newQuery)
  }

  def lookupBag(bagId: BagId, maybeVersion: Option[String]): Route =
    maybeVersion match {
      case None                => redirectToLatestVersion(bagId)
      case Some(versionString) => lookupVersionOfBag(bagId, versionString = versionString)
    }

  private def redirectToLatestVersion(bagId: BagId): Route = {
    storageManifestDao.getLatestVersion(bagId) match {
      case Right(version) =>
        redirect(
          uri = createLookupBagUrl(bagId, version = version),
          redirectionType = StatusCodes.Found
        )

      case Left(_: NoMaximaValueError) =>
        complete(
          NotFound -> UserErrorResponse(
            context = contextURL,
            statusCode = StatusCodes.NotFound,
            description = s"Storage manifest $bagId not found"
          )
        )

      case Left(readError) =>
        error(
          s"Error while trying to find the latest version of $bagId",
          readError.e
        )
        complete(
          InternalServerError -> InternalServerErrorResponse(
            context = contextURL,
            statusCode = StatusCodes.InternalServerError
          )
        )
    }
  }

  def createEtag(bagId: BagId, versionString: String): ETag =
    ETag(s"$bagId/$versionString")

  private def lookupVersionOfBag(bagId: BagId, versionString: String): Route = {
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
            ResponseDisplayBag(
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
}

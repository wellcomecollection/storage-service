package uk.ac.wellcome.platform.storage.bags.api.responses

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.ETag
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bag_tracker.client.{
  BagTrackerClient,
  BagTrackerNotFoundError,
  BagTrackerUnknownGetError
}
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagId,
  BagVersion
}
import uk.ac.wellcome.platform.archive.common.config.models.HTTPServerConfig
import uk.ac.wellcome.platform.archive.common.http.models.{
  InternalServerErrorResponse,
  UserErrorResponse
}
import uk.ac.wellcome.platform.archive.display.bags.DisplayStorageManifest

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

trait LookupBag extends Logging with ResponseBase {
  val httpServerConfig: HTTPServerConfig

  val bagTrackerClient: BagTrackerClient

  implicit val ec: ExecutionContext

  def lookupBagBetter(bagId: BagId, maybeVersionString: Option[String]): Future[Route] =
    maybeVersionString match {
      case None =>
        lookupTrackerBag(
          bagId = bagId,
          maybeVersion = None,
          notFoundMessage = s"Storage manifest $bagId not found"
        )

      case Some(versionString) =>
        parseVersion(versionString) match {
          case Success(version) =>
            lookupTrackerBag(
              bagId = bagId,
              maybeVersion = Some(version),
              notFoundMessage = s"Storage manifest $bagId $version not found"
            )

          case Failure(_) =>
            Future {
              complete(
                NotFound -> UserErrorResponse(
                  context = contextURL,
                  statusCode = StatusCodes.NotFound,
                  description = s"Storage manifest $bagId $versionString not found"
                )
              )
            }
        }
    }

  private def lookupTrackerBag(
    bagId: BagId,
    maybeVersion: Option[BagVersion],
    notFoundMessage: String): Future[Route] = {
    val response = maybeVersion match {
      case Some(version) => bagTrackerClient.getBag(bagId, version = version)
      case None          => bagTrackerClient.getLatestBag(bagId)
    }

    response.map {
      case Right(storageManifest) =>
        val etag = ETag(storageManifest.idWithVersion)

        respondWithHeader(etag) {
          complete(
            DisplayStorageManifest(
              storageManifest = storageManifest,
              contextUrl = contextURL
            )
          )
        }

      case Left(_: BagTrackerNotFoundError) =>
        complete(
          NotFound -> UserErrorResponse(
            context = contextURL,
            statusCode = StatusCodes.NotFound,
            description = notFoundMessage
          )
        )

      case Left(BagTrackerUnknownGetError(err)) =>
        error(s"Unexpected error getting bag $bagId version $maybeVersion", err)
        complete(
          InternalServerError -> InternalServerErrorResponse(
            context = contextURL,
            statusCode = StatusCodes.InternalServerError
          )
        )
    }
  }
}

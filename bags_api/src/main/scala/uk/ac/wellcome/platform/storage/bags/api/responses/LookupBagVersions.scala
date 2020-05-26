package uk.ac.wellcome.platform.storage.bags.api.responses

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bag_tracker.client.{
  BagTrackerClient,
  BagTrackerNotFoundError,
  BagTrackerUnknownListError
}
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, BagVersion}
import uk.ac.wellcome.platform.archive.common.http.models.{
  InternalServerErrorResponse,
  UserErrorResponse
}
import uk.ac.wellcome.platform.storage.bags.api.models.DisplayBagVersionList

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

trait LookupBagVersions extends Logging with ResponseBase {
  val bagTrackerClient: BagTrackerClient

  implicit val ec: ExecutionContext

  def lookupVersions(bagId: BagId, maybeBeforeString: Option[String]): Future[Route] =
    maybeBeforeString match {
      case None => lookupTrackerVersions(
        bagId = bagId,
        maybeBefore = None,
        notFoundMessage = s"No storage manifest versions found for $bagId"
      )

      case Some(versionString) =>
        parseVersion(versionString) match {
          case Success(version) =>
            lookupTrackerVersions(
              bagId = bagId,
              maybeBefore = Some(version),
              notFoundMessage = s"No storage manifest versions found for $bagId before $version"
            )

          case Failure(_) =>
            Future {
              complete(
                BadRequest -> UserErrorResponse(
                  context = contextURL,
                  statusCode = StatusCodes.BadRequest,
                  description = s"Cannot parse version string: $versionString"
                )
              )
            }
        }
    }

  private def lookupTrackerVersions(
    bagId: BagId,
    maybeBefore: Option[BagVersion],
    notFoundMessage: String): Future[Route] =
    bagTrackerClient
      .listVersionsOf(bagId = bagId, maybeBefore = maybeBefore)
      .map {
        case Right(bagVersionList) =>
          complete(
            DisplayBagVersionList(
              contextURL = contextURL,
              bagVersionList = bagVersionList
            )
          )

        case Left(_: BagTrackerNotFoundError) =>
          complete(
            NotFound -> UserErrorResponse(
              context = contextURL,
              statusCode = StatusCodes.NotFound,
              description = notFoundMessage
            )
          )

        case Left(BagTrackerUnknownListError(err)) =>
          error(s"Unexpected error looking up versions for bag ID $bagId before $maybeBefore", err)
          complete(
            InternalServerError -> InternalServerErrorResponse(
              context = contextURL,
              statusCode = StatusCodes.InternalServerError
            )
          )
      }
}

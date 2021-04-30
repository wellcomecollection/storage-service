package uk.ac.wellcome.platform.storage.bags.api.responses

import akka.http.scaladsl.model.headers.ETag
import akka.http.scaladsl.server.Route
import uk.ac.wellcome.platform.archive.bag_tracker.client.{BagTrackerClient, BagTrackerNotFoundError, BagTrackerUnknownGetError}
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, BagVersion}
import uk.ac.wellcome.platform.archive.display.bags.DisplayStorageManifest
import weco.http.FutureDirectives
import java.net.URL

import weco.http.models.HTTPServerConfig

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

trait LookupBag extends FutureDirectives {
  val httpServerConfig: HTTPServerConfig

  val bagTrackerClient: BagTrackerClient

  val contextURL: URL

  implicit val ec: ExecutionContext

  def lookupBag(
    bagId: BagId,
    maybeVersionString: Option[String]
  ): Future[Route] =
    maybeVersionString match {
      case None =>
        lookupTrackerBag(
          bagId = bagId,
          maybeVersion = None,
          notFoundMessage = s"Storage manifest $bagId not found"
        )

      case Some(versionString) =>
        BagVersion.fromString(versionString) match {
          case Success(version) =>
            lookupTrackerBag(
              bagId = bagId,
              maybeVersion = Some(version),
              notFoundMessage = s"Storage manifest $bagId $version not found"
            )

          case Failure(_) =>
            Future.successful(
              notFound(s"Storage manifest $bagId $versionString not found")
            )
        }
    }

  private def lookupTrackerBag(
    bagId: BagId,
    maybeVersion: Option[BagVersion],
    notFoundMessage: String
  ): Future[Route] = {
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

      case Left(_: BagTrackerNotFoundError) => notFound(notFoundMessage)

      case Left(BagTrackerUnknownGetError(err)) =>
        error(s"Unexpected error getting bag $bagId version $maybeVersion", err)
        internalError(err)
    }
  }
}

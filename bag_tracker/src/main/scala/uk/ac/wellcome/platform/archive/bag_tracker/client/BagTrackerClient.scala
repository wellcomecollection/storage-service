package uk.ac.wellcome.platform.archive.bag_tracker.client

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.Uri.{Path, Query}
import akka.http.scaladsl.unmarshalling.Unmarshal
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bag_tracker.models.BagVersionList
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, BagVersion}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest

import scala.concurrent.{ExecutionContext, Future}

trait BagTrackerClient {
  def createBag(
    storageManifest: StorageManifest
  ): Future[Either[BagTrackerError, Unit]]

  def getLatestBag(
    bagId: BagId
  ): Future[Either[BagTrackerError, StorageManifest]]

  def getBag(
    bagId: BagId,
    version: BagVersion
  ): Future[Either[BagTrackerError, StorageManifest]]

  def listVersionsOf(
    bagId: BagId,
    maybeBefore: Option[BagVersion]
  ): Future[Either[BagTrackerError, BagVersionList]]
}

class AkkaBagTrackerClient(trackerHost: Uri)(implicit actorSystem: ActorSystem)
    extends BagTrackerClient
    with Logging {
  implicit val ec: ExecutionContext = actorSystem.dispatcher

  override def createBag(
    storageManifest: StorageManifest
  ): Future[Either[BagTrackerCreateError, Unit]] =
    for {
      manifestEntity <- Marshal(storageManifest).to[RequestEntity]

      requestUri = trackerHost.withPath(Path("/bags"))

      request = HttpRequest(
        uri = requestUri,
        method = HttpMethods.POST,
        entity = manifestEntity
      )

      _ = info(s"Making request: $request")
      response <- Http().singleRequest(request)

      result <- response.status match {
        case StatusCodes.Created =>
          info(
            s"CREATED for POST to $requestUri with ${storageManifest.idWithVersion}"
          )
          Future(Right(()))

        case status =>
          val err = new Exception(s"$status for POST to IngestsTracker")
          error(
            f"Unexpected status for POST to $requestUri with ${storageManifest.idWithVersion}",
            err
          )
          Future(Left(BagTrackerCreateError(err)))
      }
    } yield result

  override def getLatestBag(
    bagId: BagId
  ): Future[Either[BagTrackerGetError, StorageManifest]] =
    getManifest(
      trackerHost
        .withPath(Path(s"/bags/$bagId"))
    )

  override def getBag(
    bagId: BagId,
    version: BagVersion
  ): Future[Either[BagTrackerGetError, StorageManifest]] =
    getManifest(
      trackerHost
        .withPath(Path(s"/bags/$bagId"))
        .withQuery(Query(("version", version.underlying.toString)))
    )

  private def getManifest(
    requestUri: Uri
  ): Future[Either[BagTrackerGetError, StorageManifest]] = {
    val request = HttpRequest(uri = requestUri, method = HttpMethods.GET)
    info(s"Making request: $request")

    for {
      response <- Http().singleRequest(request)

      result <- response.status match {
        case StatusCodes.OK =>
          info(s"OK for GET to $requestUri")
          Unmarshal(response.entity).to[StorageManifest].map { Right(_) }

        case StatusCodes.NotFound =>
          info(s"Not Found for GET to $requestUri")
          Future(Left(BagTrackerNotFoundError()))

        case status =>
          val err = new Throwable(s"$status from bag tracker API")
          error(s"Unexpected status from GET to $requestUri: $status", err)
          Future(Left(BagTrackerUnknownGetError(err)))
      }
    } yield result
  }

  override def listVersionsOf(
    bagId: BagId,
    maybeBefore: Option[BagVersion]
  ): Future[Either[BagTrackerListVersionsError, BagVersionList]] = {
    val baseRequestUri = trackerHost.withPath(Path(s"/bags/$bagId/versions"))

    val requestUri = maybeBefore match {
      case None => baseRequestUri
      case Some(before) =>
        baseRequestUri.withQuery(Query(("before", before.underlying.toString)))
    }

    val request = HttpRequest(uri = requestUri, method = HttpMethods.GET)

    info(s"Making request: $request")

    for {
      response <- Http().singleRequest(request)

      result <- response.status match {
        case StatusCodes.OK =>
          info(s"OK for GET to $requestUri")
          Unmarshal(response.entity).to[BagVersionList].map { Right(_) }

        case StatusCodes.NotFound =>
          info(s"Not Found for GET to $requestUri")
          Future(Left(BagTrackerNotFoundError()))

        case status =>
          val err = new Throwable(s"$status from bag tracker API")
          error(s"Unexpected status from GET to $requestUri: $status", err)
          Future(Left(BagTrackerUnknownListError(err)))
      }
    } yield result
  }
}

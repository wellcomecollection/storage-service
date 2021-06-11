package uk.ac.wellcome.platform.archive.bag_tracker.client

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import akka.stream.{Materializer, StreamTcpException}
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bag_tracker.models.BagVersionList
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, BagVersion}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.storage.RetryableError
import weco.http.client.{AkkaHttpClient, HttpClient, HttpGet, HttpPost}
import weco.http.json.CirceMarshalling

import scala.concurrent.{ExecutionContext, Future}

trait BagTrackerClient extends Logging {
  val client: HttpClient with HttpGet with HttpPost

  implicit val ec: ExecutionContext
  implicit val mat: Materializer

  def createBag(
    storageManifest: StorageManifest
  ): Future[Either[BagTrackerCreateError, Unit]] = {
    val httpResult = for {
      response <- client.post(
        path = Path("bags"),
        body = Some(storageManifest)
      )

      result <- response.status match {
        case StatusCodes.Created =>
          info(
            s"CREATED for POST to /bags with ${storageManifest.idWithVersion}"
          )
          Future(Right(()))

        case status =>
          val err = new Exception(s"$status for POST to IngestsTracker")
          error(
            f"Unexpected status for POST to /bags with ${storageManifest.idWithVersion}",
            err
          )
          Future(Left(BagTrackerCreateError(err)))
      }
    } yield result

    httpResult
      .recover {
        case err: Throwable if isRetryable(err) =>
          error(
            s"Retryable error from POST to /bags with ${storageManifest.idWithVersion}",
            err
          )
          Left(new BagTrackerCreateError(err) with RetryableError)

        case err: Throwable =>
          error(
            s"Unknown error from POST to /bags with ${storageManifest.idWithVersion}",
            err
          )
          Left(BagTrackerCreateError(err))
      }
  }

  def getLatestBag(
    bagId: BagId
  ): Future[Either[BagTrackerGetError, StorageManifest]] =
    getManifest(path = Path(s"bags/$bagId"))

  def getBag(
    bagId: BagId,
    version: BagVersion
  ): Future[Either[BagTrackerGetError, StorageManifest]] =
    getManifest(
      path = Path(s"bags/$bagId"),
      params = Map("version" -> version.underlying.toString)
    )

  implicit val um: Unmarshaller[HttpEntity, StorageManifest] = CirceMarshalling.fromDecoder[StorageManifest]

  private def getManifest(path: Path, params: Map[String, String] = Map.empty): Future[Either[BagTrackerGetError, StorageManifest]] = {
    val httpResult = for {
      response <- client.get(path, params = params)

      result <- response.status match {
        case StatusCodes.OK =>
          info(s"OK for GET to $path")
          Unmarshal(response.entity).to[StorageManifest].map { Right(_) }

        case StatusCodes.NotFound =>
          info(s"Not Found for GET to $path")
          Future(Left(BagTrackerNotFoundError()))

        case status =>
          val err = new Throwable(s"$status from bag tracker API")
          error(s"Unexpected status from GET to $path: $status", err)
          Future(Left(BagTrackerUnknownGetError(err)))
      }
    } yield result

    httpResult
      .recover {
        case err: Throwable if isRetryable(err) =>
          error(s"Retryable error from GET to $path", err)
          Left(new BagTrackerUnknownGetError(err) with RetryableError)

        case err: Throwable =>
          error(s"Unknown error from GET to $path", err)
          Left(BagTrackerUnknownGetError(err))
      }
  }

  def listVersionsOf(
    bagId: BagId,
    maybeBefore: Option[BagVersion]
  ): Future[Either[BagTrackerListVersionsError, BagVersionList]] = {
    val path = Path(s"bags/$bagId/versions")

    val params = maybeBefore match {
      case None         => Map[String, String]()
      case Some(before) => Map("before" -> before.underlying.toString)
    }

    info(s"Making request to $path with params $params")

    val httpResult = for {
      response <- client.get(path = path, params = params)

      result <- response.status match {
        case StatusCodes.OK =>
          info(s"OK for GET to $path with $params")
          Unmarshal(response.entity).to[BagVersionList].map {
            Right(_)
          }

        case StatusCodes.NotFound =>
          info(s"Not Found for GET to $path with $params")
          Future(Left(BagTrackerNotFoundError()))

        case status =>
          val err = new Throwable(s"$status from bag tracker API")
          error(s"Unexpected status from GET to $path with $params: $status", err)
          Future(Left(BagTrackerUnknownListError(err)))
      }
    } yield result

    httpResult
      .recover {
        case err: Throwable if isRetryable(err) =>
          error(s"Retryable error from GET to $path with $params", err)
          Left(new BagTrackerUnknownListError(err) with RetryableError)

        case err: Throwable =>
          error(s"Unknown error from GET to $path with $params", err)
          Left(BagTrackerUnknownListError(err))
      }
  }

  private def isRetryable(err: Throwable): Boolean =
    err match {
      // This error can occur if the tracker API is unavailable.  Tasks should wait
      // and then retry the request, if possible.
      // See https://github.com/wellcomecollection/platform/issues/4834
      case _: StreamTcpException => true

      case _ => false
    }
}

class AkkaBagTrackerClient(trackerHost: Uri)(
  implicit actorSystem: ActorSystem, val ec: ExecutionContext, val mat: Materializer)
    extends BagTrackerClient {
  override val client =
    new AkkaHttpClient() with HttpGet with HttpPost {
      override val baseUri: Uri = trackerHost
    }
}

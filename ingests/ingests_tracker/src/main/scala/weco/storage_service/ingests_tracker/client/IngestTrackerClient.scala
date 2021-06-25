package weco.storage_service.ingests_tracker.client

import akka.actor.ActorSystem
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import grizzled.slf4j.Logging
import weco.json.JsonUtil._
import weco.storage_service.ingests.models.{
  Ingest,
  IngestID,
  IngestUpdate
}
import weco.http.client.{AkkaHttpClient, HttpClient, HttpGet, HttpPost}

import scala.concurrent.{ExecutionContext, Future}

trait IngestTrackerClient extends Logging {
  val client: HttpClient with HttpGet with HttpPost

  implicit val ec: ExecutionContext
  implicit val mat: Materializer

  def createIngest(
    ingest: Ingest
  ): Future[Either[IngestTrackerCreateError, Unit]] =
    for {
      response <- client.post(
        path = Path("ingest"),
        body = Some(ingest)
      )

      result <- response.status match {
        case StatusCodes.Created =>
          info(s"CREATED for POST to /ingest with $ingest")
          Future(Right(()))
        case StatusCodes.Conflict =>
          info(s"Conflict for POST to /ingest with $ingest")
          Future(Left(IngestTrackerCreateConflictError(ingest)))
        case status =>
          val err = new Exception(s"$status for POST to IngestsTracker")
          error(f"NOT OK for POST to /ingest with $ingest", err)
          Future(Left(IngestTrackerUnknownCreateError(ingest, err)))
      }
    } yield result

  def updateIngest(
    ingestUpdate: IngestUpdate
  ): Future[Either[IngestTrackerUpdateError, Ingest]] =
    for {
      ingestUpdateEntity <- Marshal(ingestUpdate).to[RequestEntity]

      path = Path(f"/ingest/${ingestUpdate.id}")
      requestUri = client.baseUri.withPath(path)

      request = HttpRequest(
        uri = requestUri,
        method = HttpMethods.PATCH,
        entity = ingestUpdateEntity
      )

      _ = info(f"Making request: $request")

      response <- client.singleRequest(request)

      ingest <- response.status match {
        case StatusCodes.OK =>
          info(f"OK for PATCH to $requestUri with $ingestUpdate")
          Unmarshal(response.entity).to[Ingest].map(Right(_))
        case StatusCodes.NotFound =>
          warn(f"Not Found for PATCH to $requestUri with $ingestUpdate")
          Future(Left(IngestTrackerUpdateNonExistentIngestError(ingestUpdate)))
        case StatusCodes.Conflict =>
          warn(f"Conflict for PATCH to $requestUri with $ingestUpdate")
          Future(Left(IngestTrackerUpdateConflictError(ingestUpdate)))
        case status =>
          val err = new Exception(f"$status from IngestsTracker")
          error(f"NOT OK for PATCH to $requestUri with $ingestUpdate", err)
          Future(Left(IngestTrackerUnknownUpdateError(ingestUpdate, err)))
      }
    } yield ingest

  def getIngest(id: IngestID): Future[Either[IngestTrackerGetError, Ingest]] =
    for {
      response <- client.get(path = Path(s"ingest/$id"))

      ingest <- response.status match {
        case StatusCodes.OK =>
          info(s"OK for GET to /ingest/$id")
          Unmarshal(response.entity).to[Ingest].map(Right(_))
        case StatusCodes.NotFound =>
          warn(s"Not Found for GET to /ingest/$id")
          Future(Left(IngestTrackerNotFoundError(id)))
        case status =>
          val err = new Exception(s"$status from IngestsTracker")
          error(f"NOT OK for GET to /ingest/$id", err)
          Future(Left(IngestTrackerUnknownGetError(id, err)))
      }
    } yield ingest
}

class AkkaIngestTrackerClient(trackerHost: Uri)(
  implicit
  as: ActorSystem,
  val mat: Materializer,
  val ec: ExecutionContext
) extends IngestTrackerClient {
  override val client =
    new AkkaHttpClient() with HttpGet with HttpPost {
      override val baseUri: Uri = trackerHost
    }
}

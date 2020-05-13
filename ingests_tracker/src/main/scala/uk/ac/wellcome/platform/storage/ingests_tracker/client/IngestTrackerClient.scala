package uk.ac.wellcome.platform.storage.ingests_tracker.client

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  IngestUpdate
}

import scala.concurrent.{ExecutionContext, Future}

trait IngestTrackerClient {
  def updateIngest(
    ingestUpdate: IngestUpdate
  ): Future[Either[IngestTrackerError, Ingest]]
}

class AkkaIngestTrackerClient(trackerHost: Uri)(implicit as: ActorSystem)
    extends IngestTrackerClient
    with Logging {

  implicit val ec: ExecutionContext = as.dispatcher

  def updateIngest(
    ingestUpdate: IngestUpdate
  ): Future[Either[IngestTrackerError, Ingest]] =
    for {
      ingestUpdateEntity <- Marshal(ingestUpdate).to[RequestEntity]

      path = Path(f"/ingest/${ingestUpdate.id}")
      requestUri = trackerHost.withPath(path)

      request = HttpRequest(
        uri = requestUri,
        method = HttpMethods.PATCH,
        entity = ingestUpdateEntity
      )

      _ = info(f"Making request: $request")

      response <- Http().singleRequest(request)

      ingest <- response.status match {
        case StatusCodes.OK =>
          info(f"OK for PATCH to $requestUri with $ingestUpdate")
          Unmarshal(response.entity).to[Ingest].map(Right(_))
        case StatusCodes.Conflict =>
          warn(f"Conflict for PATCH to $requestUri with $ingestUpdate")
          Future(Left(IngestTrackerConflictError(ingestUpdate)))
        case status =>
          val err = new Exception(f"$status from IngestsTracker")
          error(f"NOT OK for PATCH to $requestUri with $ingestUpdate", err)
          Future(Left(IngestTrackerUnknownError(ingestUpdate, err)))
      }
    } yield ingest
}

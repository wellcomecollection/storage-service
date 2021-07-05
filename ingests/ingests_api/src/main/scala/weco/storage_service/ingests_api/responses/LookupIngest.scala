package weco.storage_service.ingests_api.responses

import akka.http.scaladsl.server.Route
import grizzled.slf4j.Logging
import weco.http.FutureDirectives
import weco.storage_service.display.ingests.ResponseDisplayIngest
import weco.storage_service.ingests.models.IngestID
import weco.storage_service.ingests_tracker.client.{
  IngestTrackerClient,
  IngestTrackerNotFoundError,
  IngestTrackerUnknownGetError
}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

trait LookupIngest extends FutureDirectives with Logging {
  val ingestTrackerClient: IngestTrackerClient

  implicit val ec: ExecutionContext

  def lookupIngest(id: UUID): Future[Route] =
    ingestTrackerClient
      .getIngest(IngestID(id))
      .map {
        case Right(ingest) =>
          complete(ResponseDisplayIngest(ingest))

        case Left(_: IngestTrackerNotFoundError) =>
          notFound(s"Ingest $id not found")

        case Left(IngestTrackerUnknownGetError(_, err)) =>
          error(s"Unexpected error from ingest tracker for $id: $err")
          internalError(err)
      }
      .recover {
        case err =>
          error(s"Unexpected error while calling ingest tracker $id: $err")
          internalError(err)
      }
}

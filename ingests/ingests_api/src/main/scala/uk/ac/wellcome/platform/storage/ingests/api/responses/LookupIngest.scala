package uk.ac.wellcome.platform.storage.ingests.api.responses

import java.net.URL
import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.display.ingests.ResponseDisplayIngest
import uk.ac.wellcome.platform.storage.ingests_tracker.client.{IngestTrackerClient, IngestTrackerNotFoundError, IngestTrackerUnknownGetError}
import weco.http.FutureDirectives
import weco.http.models.{ContextResponse, DisplayError}

import scala.concurrent.{ExecutionContext, Future}

trait LookupIngest extends FutureDirectives with Logging {
  val ingestTrackerClient: IngestTrackerClient

  implicit val ec: ExecutionContext

  def lookupIngest(id: UUID): Future[Route] =
    ingestTrackerClient
      .getIngest(IngestID(id))
      .map {
        case Right(ingest) =>
          complete(ResponseDisplayIngest(ingest, new URL(context)))
        case Left(_: IngestTrackerNotFoundError) =>
          complete(
            StatusCodes.NotFound -> ContextResponse(
              context = context,
              DisplayError(
                statusCode = StatusCodes.NotFound,
                description = s"Ingest $id not found"
              )
            )
          )
        case Left(IngestTrackerUnknownGetError(_, err)) =>
          error(s"Unexpected error from ingest tracker for $id: $err")
          complete(
            StatusCodes.InternalServerError -> ContextResponse(
              context = context,
              DisplayError(statusCode = StatusCodes.InternalServerError)
            )
          )
      }
      .recover {
        case err =>
          error(s"Unexpected error while calling ingest tracker $id: $err")
          complete(
            StatusCodes.InternalServerError -> ContextResponse(
              context = context,
              DisplayError(statusCode = StatusCodes.InternalServerError)
            )
          )
      }
}

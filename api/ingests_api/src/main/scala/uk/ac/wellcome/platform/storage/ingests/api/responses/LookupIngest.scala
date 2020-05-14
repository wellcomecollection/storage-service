package uk.ac.wellcome.platform.storage.ingests.api.responses

import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.http.models.{
  InternalServerErrorResponse,
  UserErrorResponse
}
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.display.ingests.ResponseDisplayIngest
import uk.ac.wellcome.platform.storage.ingests_tracker.client.{
  IngestTrackerClient,
  IngestTrackerNotFoundError,
  IngestTrackerUnknownGetError
}

import scala.concurrent.{ExecutionContext, Future}

trait LookupIngest extends ResponseBase with Logging {
  val ingestTrackerClient: IngestTrackerClient

  implicit val ec: ExecutionContext

  def lookupIngest(id: UUID): Future[Route] =
    ingestTrackerClient
      .getIngest(IngestID(id))
      .map {
        case Right(ingest) =>
          complete(ResponseDisplayIngest(ingest, contextURL))
        case Left(_: IngestTrackerNotFoundError) =>
          complete(
            StatusCodes.NotFound -> UserErrorResponse(
              context = contextURL,
              statusCode = StatusCodes.NotFound,
              description = s"Ingest $id not found"
            )
          )
        case Left(IngestTrackerUnknownGetError(_, err)) =>
          error(s"Unexpected error from ingest tracker for $id: $err")
          complete(
            StatusCodes.InternalServerError -> InternalServerErrorResponse(
              contextURL,
              statusCode = StatusCodes.InternalServerError
            )
          )
      }
      .recover {
        case err =>
          error(s"Unexpected error while calling ingest tracker $id: $err")
          complete(
            StatusCodes.InternalServerError -> InternalServerErrorResponse(
              contextURL,
              statusCode = StatusCodes.InternalServerError
            )
          )
      }
}

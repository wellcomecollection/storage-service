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
import uk.ac.wellcome.platform.archive.common.ingests.tracker.{
  IngestDoesNotExistError,
  IngestTracker
}
import uk.ac.wellcome.platform.archive.display.ResponseDisplayIngest

trait LookupIngest extends ResponseBase with Logging {
  val ingestTracker: IngestTracker

  def lookupIngest(id: UUID): Route =
    ingestTracker.get(IngestID(id)) match {
      case Right(ingest) =>
        complete(ResponseDisplayIngest(ingest.identifiedT, contextURL))
      case Left(_: IngestDoesNotExistError) =>
        complete(
          StatusCodes.NotFound -> UserErrorResponse(
            context = contextURL,
            statusCode = StatusCodes.NotFound,
            description = s"Ingest $id not found"
          )
        )
      case Left(err) =>
        error(s"Unexpected error while fetching ingest $id: $err")
        complete(
          StatusCodes.InternalServerError -> InternalServerErrorResponse(
            contextURL,
            statusCode = StatusCodes.InternalServerError
          )
        )
    }
}

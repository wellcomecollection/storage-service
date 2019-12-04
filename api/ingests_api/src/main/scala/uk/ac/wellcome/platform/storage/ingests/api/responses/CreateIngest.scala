package uk.ac.wellcome.platform.storage.ingests.api.responses

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.config.models.HTTPServerConfig
import uk.ac.wellcome.platform.archive.common.http.models.{InternalServerErrorResponse, UserErrorResponse}
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.display.{RequestDisplayIngest, ResponseDisplayIngest}
import uk.ac.wellcome.platform.storage.ingests.api.services.IngestStarter

import scala.util.{Failure, Success}

trait CreateIngest extends ResponseBase with Logging {
  val httpServerConfig: HTTPServerConfig
  val ingestStarter: IngestStarter[_]

  def createIngest(requestDisplayIngest: RequestDisplayIngest): Route =

    // We disallow slashes for space/external identifier.
    //
    // In theory there's nothing that means we can't support it, but it's liable
    // to cause a bunch of issues (e.g. when we build S3 paths, a slash is a
    // path separator).  If there's a pressing need, we can go through and check
    // the pipeline handles slashes correctly -- e.g. URL encoding them where
    // necessary, ensuring they can't clash -- but for now, we just stop them
    // ever entering the pipeline.
    //
    if (requestDisplayIngest.space.id.contains("/")) {
      complete(
        StatusCodes.BadRequest -> UserErrorResponse(
          contextURL,
          statusCode = StatusCodes.BadRequest,
          description =
            "Invalid value at .space.id: must not contain slashes."
        )
      )
    } else if (requestDisplayIngest.bag.info.externalIdentifier.underlying
      .contains("/")) {
      complete(
        StatusCodes.BadRequest -> UserErrorResponse(
          contextURL,
          statusCode = StatusCodes.BadRequest,
          description =
            "Invalid value at .bag.info.externalIdentifier: must not contain slashes."
        )
      )
    } else {
      triggerIngestStarter(requestDisplayIngest)
    }

  private def triggerIngestStarter(requestDisplayIngest: RequestDisplayIngest): Route =
    ingestStarter.initialise(requestDisplayIngest.toIngest) match {
      case Success(ingest) =>
        respondWithHeaders(List(createLocationHeader(ingest))) {
          complete(
            StatusCodes.Created -> ResponseDisplayIngest(
              ingest,
              contextURL
            )
          )
        }
      case Failure(err) =>
        error(
          s"Unexpected error while creating an ingest $requestDisplayIngest",
          err
        )
        complete(
          StatusCodes.InternalServerError -> InternalServerErrorResponse(
            contextURL,
            statusCode = StatusCodes.InternalServerError
          )
        )
    }

  private def createLocationHeader(ingest: Ingest) =
    Location(s"${httpServerConfig.externalBaseURL}/${ingest.id}")
}

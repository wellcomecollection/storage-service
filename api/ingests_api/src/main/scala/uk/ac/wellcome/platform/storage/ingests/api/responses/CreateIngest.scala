package uk.ac.wellcome.platform.storage.ingests.api.responses

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.config.models.HTTPServerConfig
import uk.ac.wellcome.platform.archive.common.http.models.{
  InternalServerErrorResponse,
  UserErrorResponse
}
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  StorageProvider
}
import uk.ac.wellcome.platform.archive.display.ingests.{
  RequestDisplayIngest,
  ResponseDisplayIngest
}
import uk.ac.wellcome.platform.storage.ingests.api.services.IngestStarter

import scala.util.{Failure, Success}

trait CreateIngest extends ResponseBase with Logging {
  val httpServerConfig: HTTPServerConfig
  val ingestStarter: IngestStarter[_]

  def createIngest(requestDisplayIngest: RequestDisplayIngest): Route = {
    val space = requestDisplayIngest.space.id
    val externalIdentifier = requestDisplayIngest.bag.info.externalIdentifier
    val providerId = requestDisplayIngest.sourceLocation.provider.id

    if (space.contains("/")) {
      createBadRequestResponse(
        "Invalid value at .space.id: must not contain slashes."
      )
    } else if (space == "") {
      createBadRequestResponse("Invalid value at .space.id: must not be empty.")
    } else if (externalIdentifier == "") {
      createBadRequestResponse(
        "Invalid value at .bag.info.externalIdentifier: must not be empty."
      )
    } else if (!StorageProvider.allowedValues.contains(providerId)) {
      createBadRequestResponse(
        s"""Unrecognised value at .sourceLocation.provider.id: got "$providerId", valid values are: ${StorageProvider.allowedValues
          .mkString(", ")}."""
      )
    } else {
      triggerIngestStarter(requestDisplayIngest)
    }
  }

  private def createBadRequestResponse(description: String): Route =
    complete(
      StatusCodes.BadRequest -> UserErrorResponse(
        contextURL,
        statusCode = StatusCodes.BadRequest,
        description = description
      )
    )

  private def triggerIngestStarter(
    requestDisplayIngest: RequestDisplayIngest
  ): Route =
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

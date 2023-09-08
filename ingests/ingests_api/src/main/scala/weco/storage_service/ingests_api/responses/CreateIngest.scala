package weco.storage_service.ingests_api.responses

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server.Route
import grizzled.slf4j.Logging
import weco.storage_service.ingests.models.{
  AmazonS3StorageProvider,
  Ingest,
  StorageProvider
}
import weco.storage_service.display.ingests.{
  RequestDisplayIngest,
  ResponseDisplayIngest
}
import weco.storage_service.ingests_api.services.IngestCreator
import weco.http.FutureDirectives
import weco.http.models.{DisplayError, HTTPServerConfig}

import java.lang.IllegalArgumentException
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

trait CreateIngest[UnpackerDestination] extends FutureDirectives with Logging {
  val httpServerConfig: HTTPServerConfig
  val ingestCreator: IngestCreator[UnpackerDestination]

  implicit val ec: ExecutionContext

  def createIngest(
    requestDisplayIngest: RequestDisplayIngest
  ): Future[Route] = {
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
        s"""Unrecognised value at .sourceLocation.provider.id: got "$providerId", valid values are: ${StorageProvider.recognisedValues
          .mkString(", ")}."""
      )
    }
    // In theory we might support unpacking bags uploaded to a different location
    // at some point, but right now we can only unpack bags from S3.
    //
    // Fail immediately rather than passing unusable requests into the pipeline.
    else if (StorageProvider.apply(providerId) != AmazonS3StorageProvider) {
      createBadRequestResponse(
        "Forbidden value at .sourceLocation.provider.id: only amazon-s3 is supported for new bags."
      )
    } else {
      triggerIngestStarter(requestDisplayIngest)
    }
  }

  private def createBadRequestResponse(description: String): Future[Route] =
    Future.successful(
      invalidRequest(description)
    )

  private def convertToBadRequestResponse(
    invalidArg: IllegalArgumentException): Future[Route] = {
    def originalMessage = invalidArg.getMessage
    def prefix = "requirement failed: External identifier"
    def newMessage =
      if (originalMessage.startsWith(prefix))
        "Invalid value at .bag.info.externalIdentifier:" + originalMessage
          .stripPrefix(prefix)
      else originalMessage

    createBadRequestResponse(newMessage)
  }

  private def triggerIngestStarter(
    requestDisplayIngest: RequestDisplayIngest
  ): Future[Route] = {

    Try {
      requestDisplayIngest.toIngest
    } match {
      case Failure(exception) =>
        exception match {
          case invalidArg: IllegalArgumentException =>
            convertToBadRequestResponse(invalidArg)
          case otherException => throw otherException
        }
      case Success(ingest) => triggerIngestStarter(ingest)
    }
  }

  private def triggerIngestStarter(ingest: Ingest): Future[Route] = {
    val creationResult = ingestCreator.create(ingest)

    creationResult
      .map {
        case Right(()) =>
          val headers = List(
            Location(s"${httpServerConfig.externalBaseURL}/${ingest.id}")
          )

          respondWithHeaders(headers) {
            complete(
              StatusCodes.Created -> ResponseDisplayIngest(ingest)
            )
          }

        // This will capture both a Conflict and an Internal error from the ingest
        // tracker.  There's nothing a user can do about either of them, so return
        // them both as 500 errors.
        case Left(err) =>
          complete(
            StatusCodes.InternalServerError -> DisplayError(
              statusCode = StatusCodes.InternalServerError
            )
          )
      }
      .recover {
        case err =>
          error(s"Unexpected error while creating ingest $ingest: $err")
          internalError(err)
      }
  }

}

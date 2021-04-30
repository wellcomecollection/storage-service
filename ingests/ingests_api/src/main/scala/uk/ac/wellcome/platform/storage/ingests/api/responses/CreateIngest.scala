package uk.ac.wellcome.platform.storage.ingests.api.responses

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.MessageSender
import uk.ac.wellcome.platform.archive.common.SourceLocationPayload
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  AmazonS3StorageProvider,
  StorageProvider
}
import uk.ac.wellcome.platform.archive.display.ingests.{
  RequestDisplayIngest,
  ResponseDisplayIngest
}
import uk.ac.wellcome.platform.storage.ingests_tracker.client.IngestTrackerClient
import weco.http.models.{ContextResponse, DisplayError, HTTPServerConfig}

import scala.concurrent.{ExecutionContext, Future}

trait CreateIngest[UnpackerDestination] extends ResponseBase with Logging {
  val httpServerConfig: HTTPServerConfig
  val ingestTrackerClient: IngestTrackerClient
  val unpackerMessageSender: MessageSender[UnpackerDestination]

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
    Future {
      complete(
        StatusCodes.BadRequest -> ContextResponse(
          context = contextURL,
          DisplayError(
            statusCode = StatusCodes.BadRequest,
            description = description
          )
        )
      )
    }

  private def triggerIngestStarter(
    requestDisplayIngest: RequestDisplayIngest
  ): Future[Route] = {
    val ingest = requestDisplayIngest.toIngest

    val creationResult = for {
      trackerResult <- ingestTrackerClient.createIngest(ingest)
      _ <- trackerResult match {
        case Right(_) =>
          Future.fromTry {
            unpackerMessageSender.sendT(SourceLocationPayload(ingest))
          }
        case Left(_) => Future.successful(())
      }
    } yield trackerResult

    creationResult
      .map {
        case Right(()) =>
          val headers = List(
            Location(s"${httpServerConfig.externalBaseURL}/${ingest.id}")
          )

          respondWithHeaders(headers) {
            complete(
              StatusCodes.Created -> ResponseDisplayIngest(
                ingest,
                contextURL
              )
            )
          }

        // This will capture both a Conflict and an Internal error from the ingest
        // tracker.  There's nothing a user can do about either of them, so return
        // them both as 500 errors.
        case Left(_) =>
          complete(
            StatusCodes.InternalServerError -> ContextResponse(
              context = contextURL,
              DisplayError(statusCode = StatusCodes.InternalServerError)
            )
          )
      }
      .recover {
        case err =>
          error(s"Unexpected error while creating ingest $ingest: $err")
          complete(
            StatusCodes.InternalServerError -> ContextResponse(
              context = contextURL,
              DisplayError(statusCode = StatusCodes.InternalServerError)
            )
          )
      }
  }
}

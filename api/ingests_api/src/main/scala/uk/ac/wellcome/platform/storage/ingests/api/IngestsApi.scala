package uk.ac.wellcome.platform.storage.ingests.api

import java.net.URL
import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server._
import grizzled.slf4j.Logging
import io.circe.Printer
import uk.ac.wellcome.platform.archive.common.config.models.HTTPServerConfig
import uk.ac.wellcome.platform.archive.common.http.models.{
  InternalServerErrorResponse,
  UserErrorResponse
}
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.{
  IngestDoesNotExistError,
  IngestTracker
}
import uk.ac.wellcome.platform.archive.display.{
  RequestDisplayIngest,
  ResponseDisplayIngest
}
import uk.ac.wellcome.platform.storage.ingests.api.services.IngestStarter

import scala.util.{Failure, Success}

trait IngestsApi extends Logging {

  val ingestTracker: IngestTracker
  val ingestStarter: IngestStarter[_]
  val httpServerConfig: HTTPServerConfig
  val contextURL: URL

  import akka.http.scaladsl.server.Directives._
  import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
  import uk.ac.wellcome.json.JsonUtil._

  implicit val printer: Printer = Printer.noSpaces.copy(dropNullValues = true)

  val ingests: Route = pathPrefix("ingests") {
    post {
      entity(as[RequestDisplayIngest]) { requestDisplayIngest =>
        // TODO: Do we have a test for the failure case?
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
      }
    } ~ path(JavaUUID) { id: UUID =>
      get {
        // TODO: Do we have a test for the failure case?
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
    }
  }

  private def createLocationHeader(ingest: Ingest) =
    Location(s"${httpServerConfig.externalBaseURL}/${ingest.id}")
}

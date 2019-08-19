package uk.ac.wellcome.platform.storage.ingests.api

import java.net.URL
import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server._
import grizzled.slf4j.Logging
import io.circe.Printer
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, ExternalIdentifier}
import uk.ac.wellcome.platform.archive.common.config.models.HTTPServerConfig
import uk.ac.wellcome.platform.archive.common.http.models.{InternalServerErrorResponse, UserErrorResponse}
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.{IngestDoesNotExistError, IngestTracker}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.archive.display.{DisplayIngestMinimal, RequestDisplayIngest, ResponseDisplayIngest}
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
      } ~ path("find-by-bag-id" / Segment) { combinedId: String =>
        // Temporary route to match colon separated ids '/find-by-bag-id/storageSpace:bagId' used by DLCS
        // remove when DLCS replaces this by '/find-by-bag-id/storageSpace/bagId'
        get {
          val parts = combinedId.split(':')
          val bagId =
            BagId(StorageSpace(parts.head), ExternalIdentifier(parts.last))
          findIngest(bagId)
        }
      } ~ path("find-by-bag-id" / Segment / Segment) { (space, id) =>
        // Route used by DLCS to find ingests for a bag, not part of the public/documented API.  Either remove
        // if no longer needed after migration or enhance and document as part of the API.
        get {
          val bagId = BagId(StorageSpace(space), ExternalIdentifier(id))
          findIngest(bagId)
        }
      }
    }

  private def findIngest(bagId: BagId): StandardRoute =
    ingestTracker.listByBagId(bagId) match {
      case Right(results) =>
        if (results.nonEmpty) {
          complete(StatusCodes.OK -> results.map { DisplayIngestMinimal(_) })
        } else {
          complete(StatusCodes.NotFound -> List[DisplayIngestMinimal]())
        }

      case Left(err) =>
        warn(s"""errors fetching ingests for $bagId: $err""")
        complete(
          StatusCodes.InternalServerError -> InternalServerErrorResponse(
            context = contextURL,
            statusCode = StatusCodes.InternalServerError
          )
        )
    }

  private def createLocationHeader(ingest: Ingest) =
    Location(s"${httpServerConfig.externalBaseURL}/${ingest.id}")
}

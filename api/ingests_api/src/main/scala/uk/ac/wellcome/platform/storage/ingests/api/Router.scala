package uk.ac.wellcome.platform.storage.ingests.api

import java.net.URL
import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server._
import grizzled.slf4j.Logging
import io.circe.Printer
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagId,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.config.models.HTTPServerConfig
import uk.ac.wellcome.platform.archive.common.http.models.{
  InternalServerErrorResponse,
  UserErrorResponse
}
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.common.ingests.monitor.IngestTracker
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.archive.display.{
  DisplayIngestMinimal,
  RequestDisplayIngest,
  ResponseDisplayIngest
}

class Router(ingestTracker: IngestTracker,
             ingestStarter: IngestStarter,
             httpServerConfig: HTTPServerConfig,
             contextURL: URL)
    extends Logging {

  import akka.http.scaladsl.server.Directives._
  import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
  import uk.ac.wellcome.json.JsonUtil._

  implicit val printer: Printer = Printer.noSpaces.copy(dropNullValues = true)

  def routes: Route =
    pathPrefix("progress") {
      post {
        entity(as[RequestDisplayIngest]) { requestDisplayIngest =>
          onSuccess(ingestStarter.initialise(requestDisplayIngest.toIngest)) {
            ingest =>
              respondWithHeaders(List(createLocationHeader(ingest))) {
                complete(Created -> ResponseDisplayIngest(ingest, contextURL))
              }
          }
        }
      } ~ path(JavaUUID) { id: UUID =>
        get {
          onSuccess(ingestTracker.get(id)) {
            case Some(ingest) =>
              complete(ResponseDisplayIngest(ingest, contextURL))
            case None =>
              complete(
                NotFound -> UserErrorResponse(
                  context = contextURL,
                  statusCode = StatusCodes.NotFound,
                  description = s"Ingest $id not found"
                ))
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

  private def findIngest(bagId: BagId) = {
    val results = ingestTracker.findByBagId(bagId)
    if (results.nonEmpty && results.forall(_.isRight)) {
      complete(OK -> results.collect {
        case Right(ingest) => DisplayIngestMinimal(ingest)
      })
    } else if (results.isEmpty) {
      complete(NotFound -> List[DisplayIngestMinimal]())
    } else {
      info(s"""errors fetching ingests for $bagId: ${results.mkString(" ")}""")
      complete(
        InternalServerError -> InternalServerErrorResponse(
          context = contextURL,
          statusCode = InternalServerError
        )
      )
    }
  }

  private def createLocationHeader(ingest: Ingest) =
    Location(s"${httpServerConfig.externalBaseURL}/${ingest.id}")
}

package uk.ac.wellcome.platform.storage.bags.api

import java.net.URL

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import grizzled.slf4j.Logging
import io.circe.Printer
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagId,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.http.models.{
  InternalServerErrorResponse,
  UserErrorResponse
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestVHS
import uk.ac.wellcome.platform.storage.bags.api.models.DisplayBag

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class Router(vhs: StorageManifestVHS, contextURL: URL)(
  implicit val ec: ExecutionContext)
    extends Logging {

  def routes: Route = {
    import akka.http.scaladsl.server.Directives._
    import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
    implicit val printer: Printer = Printer.noSpaces.copy(dropNullValues = true)

    pathPrefix("bags") {
      path(Segment / Segment) { (space, externalIdentifier) =>
        val bagId = BagId(
          space = StorageSpace(space),
          externalIdentifier = ExternalIdentifier(externalIdentifier)
        )

        get {
          vhs.getRecord(bagId) match {
            case Success(result) =>
              result match {
                case Some(storageManifest) =>
                  complete(DisplayBag(storageManifest, contextURL))
                case None =>
                  complete(
                    StatusCodes.NotFound -> UserErrorResponse(
                      context = contextURL,
                      statusCode = StatusCodes.NotFound,
                      description = s"Storage manifest $bagId not found"
                    ))
              }
            case Failure(t) =>
              error(s"Error looking up storage manifest $bagId", t)
              complete(
                StatusCodes.InternalServerError -> InternalServerErrorResponse(
                  contextURL)
              )
          }
        }
      }
    }
  }
}

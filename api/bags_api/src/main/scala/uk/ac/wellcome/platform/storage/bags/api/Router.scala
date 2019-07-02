package uk.ac.wellcome.platform.storage.bags.api

import java.net.URL

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.StatusCodes._
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
import uk.ac.wellcome.platform.archive.common.storage.models.{
  StorageManifest,
  StorageSpace
}
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao
import uk.ac.wellcome.platform.storage.bags.api.models.DisplayBag
import uk.ac.wellcome.storage.{NoVersionExistsError, ReadError}

import scala.concurrent.ExecutionContext

class Router(register: StorageManifestDao, contextURL: URL)(
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
          parameter('version.?) { maybeVersion =>
            val result: Either[ReadError, StorageManifest] = maybeVersion match {
              case Some(version) => register.get(bagId, version = version.toInt)
              case None          => register.getLatest(bagId)
            }

            result match {
              case Right(storageManifest) =>
                complete(
                  DisplayBag(
                    storageManifest = storageManifest,
                    contextUrl = contextURL)
                )
              case Left(_: NoVersionExistsError) =>
                val errorMessage = maybeVersion match {
                  case Some(version) => s"Storage manifest $bagId v$version not found"
                  case None          => s"Storage manifest $bagId not found"
                }

                complete(
                  NotFound -> UserErrorResponse(
                    context = contextURL,
                    statusCode = StatusCodes.NotFound,
                    description = errorMessage
                  )
                )
              case Left(storageError) =>
                error("Internal server error", storageError.e)
                complete(
                  InternalServerError -> InternalServerErrorResponse(
                    context = contextURL,
                    statusCode = StatusCodes.InternalServerError
                  )
                )
            }
          }
        }
      }
    }
  }
}

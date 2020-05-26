package uk.ac.wellcome.platform.storage.bags.api.responses

import java.net.URL

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{complete, onComplete}
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import uk.ac.wellcome.json.JsonUtil._
import io.circe.Printer
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
import uk.ac.wellcome.platform.archive.common.http.models.InternalServerErrorResponse

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex

trait ResponseBase {
  implicit val printer: Printer =
    Printer.noSpaces.copy(dropNullValues = true)

  val contextURL: URL

  // Matches a version string.  The versions from the storage service
  // are always of the form 'v1', 'v2', ...
  private val versionRegex: Regex = new Regex("^v(\\d+)$", "version")

  def parseVersion(versionString: String): Try[BagVersion] =
    versionRegex.findFirstMatchIn(versionString) match {
      case Some(regexMatch) =>
        Success(
          BagVersion(regexMatch.group("version").toInt)
        )
      case None =>
        Failure(
          new Throwable(s"Could not parse version string: $versionString")
        )
    }

  protected def withFuture(future: Future[Route]): Route =
    onComplete(future) {
      case Success(resp) => resp
      case Failure(_) =>
        complete(
          StatusCodes.InternalServerError -> InternalServerErrorResponse(
            contextURL,
            statusCode = StatusCodes.InternalServerError
          )
        )
    }
}

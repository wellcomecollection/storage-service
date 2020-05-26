package uk.ac.wellcome.platform.storage.bags.api.responses

import io.circe.Printer
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion

import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex

trait ResponseBase {
  implicit val printer: Printer =
    Printer.noSpaces.copy(dropNullValues = true)

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
}

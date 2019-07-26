package uk.ac.wellcome.platform.storage.bags.api

import java.net.URL

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Route
import grizzled.slf4j.Logging
import io.circe.Printer
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, BagVersion, ExternalIdentifier}
import uk.ac.wellcome.platform.archive.common.http.models.{InternalServerErrorResponse, UserErrorResponse}
import uk.ac.wellcome.platform.archive.common.storage.models.{StorageManifest, StorageSpace}
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao
import uk.ac.wellcome.platform.storage.bags.api.models.{DisplayResultList, ResponseDisplayBag, ResultListEntry}
import uk.ac.wellcome.storage.{NoVersionExistsError, ReadError}

import scala.concurrent.ExecutionContext
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

class Router(storageManifestDao: StorageManifestDao, contextURL: URL)(
  implicit val ec: ExecutionContext)
    extends Logging {

  def routes: Route = {
    import akka.http.scaladsl.server.Directives._
    import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
    implicit val printer: Printer =
      Printer.noSpaces.copy(dropNullValues = true)

    pathPrefix("bags") {
      path(Segment / Segment) { (space, externalIdentifier) =>
        val bagId = BagId(
          space = StorageSpace(space),
          externalIdentifier = ExternalIdentifier(externalIdentifier)
        )

        get {
          parameter('version.as[String] ?) { maybeVersion =>
            val result = parseVersion(maybeVersion) match {
              case Success(Some(version)) =>
                storageManifestDao.get(bagId, version = version)

              case Success(None) =>
                storageManifestDao.getLatest(bagId)

              case Failure(_) =>
                Left(NoVersionExistsError())
            }

            result match {
              case Right(storageManifest) =>
                complete(
                  ResponseDisplayBag(
                    storageManifest = storageManifest,
                    contextUrl = contextURL)
                )
              case Left(_: NoVersionExistsError) =>
                val errorMessage = maybeVersion match {
                  case Some(version) =>
                    s"Storage manifest $bagId version $version not found"
                  case None => s"Storage manifest $bagId not found"
                }

                complete(
                  NotFound -> UserErrorResponse(
                    context = contextURL,
                    statusCode = StatusCodes.NotFound,
                    description = errorMessage
                  )
                )
              case Left(storageError) =>
                error(
                  s"Error while trying to look up $bagId v = $maybeVersion",
                  storageError.e)
                complete(
                  InternalServerError -> InternalServerErrorResponse(
                    context = contextURL,
                    statusCode = StatusCodes.InternalServerError
                  )
                )
            }
          }
        }
      } ~ path(Segment / Segment / "versions") { (space, externalIdentifier) =>
        val bagId = BagId(
          space = StorageSpace(space),
          externalIdentifier = ExternalIdentifier(externalIdentifier)
        )

        def buildResultsList(
          matchingManifests: Either[ReadError, Seq[StorageManifest]],
          notFoundMessage: String) =
          matchingManifests match {
            case Right(Nil) =>
              complete(
                NotFound -> UserErrorResponse(
                  context = contextURL,
                  statusCode = StatusCodes.NotFound,
                  description = notFoundMessage
                )
              )

            case Right(manifests) =>
              complete(
                DisplayResultList(
                  context = contextURL.toString,
                  results = manifests.map { ResultListEntry(_) }
                )
              )

            case Left(err) =>
              error(s"Error while trying to look up versions of $bagId", err.e)
              complete(
                InternalServerError -> InternalServerErrorResponse(
                  context = contextURL,
                  statusCode = StatusCodes.InternalServerError
                )
              )
          }

        get {
          parameter('before.as[String] ?) { maybeVersion =>
            parseVersion(maybeVersion) match {
              case Success(Some(version)) =>
                buildResultsList(
                  storageManifestDao.listVersions(bagId, before = version),
                  notFoundMessage =
                    s"No storage manifest versions found for $bagId before $version"
                )

              case Success(None) =>
                buildResultsList(
                  storageManifestDao.listVersions(bagId),
                  notFoundMessage =
                    s"No storage manifest versions found for $bagId"
                )

              // Note: if the version is empty, we'll always be able to parse it,
              // so the .get here is safe.
              case Failure(err) =>
                complete(
                  BadRequest -> UserErrorResponse(
                    context = contextURL,
                    statusCode = StatusCodes.BadRequest,
                    description =
                      s"Cannot parse version string: ${maybeVersion.get}"
                  )
                )
            }
          }
        }
      }
    }
  }

  private val versionRegex: Regex = new Regex("^v(\\d+)$", "version")

  private def parseVersion(queryParam: Option[String]): Try[Option[BagVersion]] =
    queryParam match {
      case Some(versionString) =>
        versionRegex.findFirstMatchIn(versionString) match {
          case Some(regexMatch) =>
            Success(Some(
              BagVersion(regexMatch.group("version").toInt)
            ))
          case None =>
            Failure(
              new Throwable(s"Could not parse version string: $versionString"))
        }

      case None => Success(None)
    }
}

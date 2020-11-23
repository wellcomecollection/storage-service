package uk.ac.wellcome.platform.storage.bags.api

import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagId,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.http.LookupExternalIdentifier
import uk.ac.wellcome.platform.archive.common.storage.LargeResponses
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.storage.bags.api.responses.{
  LookupBag,
  LookupBagVersions
}
import uk.ac.wellcome.storage.s3.S3ObjectLocation

import scala.concurrent.duration._

trait BagsApi
    extends LargeResponses
    with LookupBag
    with LookupBagVersions
    with LookupExternalIdentifier {

  private val routes: Route = pathPrefix("bags") {
    concat(
      // We look for /versions at the end of the path: this means we should
      // return a list of versions, not the complete manifest.
      //
      // The trailing slash is significant: it causes Akka to consume "/versions",
      // rather than "versions".  If you omit it, the externalIdentifier gets a
      // slash appended!
      //
      pathSuffix("versions" /) {
        path(Segment / Remaining) { (space, remaining) =>
          val bagId = BagId(
            space = StorageSpace(space),
            externalIdentifier = decodeExternalIdentifier(remaining)
          )

          get {
            parameter('before.as[String] ?) { maybeBefore =>
              withFuture {
                lookupVersions(
                  bagId = bagId,
                  maybeBeforeString = maybeBefore
                )
              }
            }
          }
        }
      },
      // Look up a single manifest.
      path(Segment / Remaining) { (space, remaining) =>
        val bagId = BagId(
          space = StorageSpace(space),
          externalIdentifier = decodeExternalIdentifier(remaining)
        )

        val chemistAndDruggist = BagId(
          space = StorageSpace("digitised"),
          externalIdentifier = ExternalIdentifier("b19974760")
        )

        get {
          // This is some special casing to handle Chemist & Druggist, which
          // is enormous.  If somebody tries to retrieve it, direct them straight
          // to the cached response.
          //
          // We should fix the bags API so retrieving this API doesn't cause the
          // app to run out of heap space/memory.
          //
          // See https://github.com/wellcomecollection/platform/issues/4549
          bagId match {
            case id if id == chemistAndDruggist =>
              val url = s3Uploader
                .getPresignedGetURL(
                  location = S3ObjectLocation(
                    bucket =
                      "wellcomecollection-storage-prod-large-response-cache",
                    key = "responses/digitised/b19974760/v1"
                  ),
                  expiryLength = 1 days
                )
                .right
                .get

              complete(
                HttpResponse(
                  status = StatusCodes.TemporaryRedirect,
                  headers = Location(url.toExternalForm) :: Nil
                )
              )
            case _ =>
              parameter('version.as[String] ?) { maybeVersionString =>
                withFuture {
                  lookupBag(
                    bagId = bagId,
                    maybeVersionString = maybeVersionString
                  )
                }
              }
          }
        }
      }
    )
  }

  val bags: Route = wrapLargeResponses(routes)
}

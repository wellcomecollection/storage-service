package uk.ac.wellcome.platform.storage.bags.api

import java.net.URLDecoder
import java.nio.charset.StandardCharsets

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagId,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.storage.LargeResponses
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.storage.bags.api.responses.{
  LookupBag,
  LookupBagVersions
}
import uk.ac.wellcome.storage.ObjectLocation

import scala.concurrent.duration._

trait BagsApi extends LargeResponses with LookupBag with LookupBagVersions {
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

          val chemistAndDruggist = BagId(
            space = StorageSpace("digitised"),
            externalIdentifier = ExternalIdentifier("b19974760")
          )

          get {
            parameter('before.as[String] ?) { maybeBefore =>

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
                  val url = s3Uploader.getPresignedGetURL(
                    location = ObjectLocation(
                      namespace = "wellcomecollection-storage-prod-large-response-cache",
                      path = "responses/digitised/b19974760/v1"
                    ),
                    expiryLength = 1 days
                  ).right.get

                  complete(
                    HttpResponse(
                      status = StatusCodes.TemporaryRedirect,
                      headers = Location(url.toExternalForm) :: Nil
                    )
                  )

                case _ =>
                  withFuture {
                    lookupVersions(bagId = bagId, maybeBeforeString = maybeBefore)
                  }
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

        get {
          parameter('version.as[String] ?) { maybeVersionString =>
            withFuture {
              lookupBag(bagId = bagId, maybeVersionString = maybeVersionString)
            }
          }
        }
      }
    )
  }

  private def decodeExternalIdentifier(
    remaining: String
  ): ExternalIdentifier = {
    // Sometimes we have an external identifier with slashes.
    // For maximum flexibility, we want to support both URL-encoded
    // and complete path versions.
    //
    // For example, if the external identifier is "alfa/bravo",
    // you could look up both of:
    //
    //    /bags/space-id/alfa/bravo
    //    /bags/space-id/alfa%2Fbravo
    //
    val underlying =
      URLDecoder.decode(remaining, StandardCharsets.UTF_8.toString)

    ExternalIdentifier(underlying)
  }

  val bags: Route = wrapLargeResponses(routes)
}

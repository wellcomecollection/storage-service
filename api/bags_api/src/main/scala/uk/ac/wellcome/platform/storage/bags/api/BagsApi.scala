package uk.ac.wellcome.platform.storage.bags.api

import java.net.URLDecoder
import java.nio.charset.StandardCharsets

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagId,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.storage.LargeResponses
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.storage.bags.api.lookups.{
  LookupBag,
  LookupBagVersions
}

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

          get {
            parameter('before.as[String] ?) { maybeBefore =>
              lookupVersions(bagId = bagId, maybeBefore = maybeBefore)
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
          parameter('version.as[String] ?) { maybeVersion =>
            lookupBag(bagId = bagId, maybeVersion = maybeVersion)
          }
        }
      }
    )
  }

  private def decodeExternalIdentifier(remaining: String): ExternalIdentifier = {
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

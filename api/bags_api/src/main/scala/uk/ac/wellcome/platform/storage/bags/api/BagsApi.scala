package uk.ac.wellcome.platform.storage.bags.api

import java.net.{URL, URLDecoder}
import java.nio.charset.StandardCharsets

import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagId,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.storage.LargeResponses
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.archive.common.storage.services.S3ObjectExists
import uk.ac.wellcome.platform.storage.bags.api.responses.{
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
          parameter('version.as[String] ?) {
            case None                => getLatestBag(bagId = bagId)
            case Some(versionString) => getBag(bagId = bagId, versionString = versionString)
          }
        }
      }
    )
  }

  private def getLatestBag(bagId: BagId): Route =
    getLatestVersion(bagId) match {
      case Left(route)          => route
      case Right(latestVersion) => getBag(bagId, versionString = latestVersion.toString)
    }

  /** Some of the bags are very large (Chemist & Druggist is ~180MB for the manifest
    * alone).  If we've already cached a copy of this bag in S3, we know this bag
    * is big and we won't be able to return it in time.  Return a pre-signed URL
    * for the cached response.
    *
    * If the response hasn't been cached yet, fetch a response directly from VHS.
    *
    */
  private def getBag(bagId: BagId, versionString: String): Route = {
    val etag = createEtag(bagId = bagId, versionString = versionString)

    val cacheLocation = prefix.asLocation(etag.value)

    implicit val s3Client: AmazonS3 = s3Uploader.s3Client
    import S3ObjectExists._

    cacheLocation.exists match {
      case Right(true) =>
        s3Uploader.getPresignedGetURL(
          location = cacheLocation,
          expiryLength = cacheDuration
        ) match {
          case Right(url: URL) =>
            redirect(
              uri = Uri(url.toString),
              redirectionType = StatusCodes.Found
            )

          case Left(_) => lookupBag(bagId = bagId, versionString = versionString)
        }

      case _ => lookupBag(bagId = bagId, versionString = versionString)
    }
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

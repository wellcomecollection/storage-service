package uk.ac.wellcome.platform.storage.bags.api

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, ExternalIdentifier}
import uk.ac.wellcome.platform.archive.common.storage.LargeResponses
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.storage.bags.api.lookups.{LookupBag, LookupBagVersions}

trait BagsApi extends LargeResponses with LookupBag with LookupBagVersions {
  private val routes: Route = pathPrefix("bags") {
    path(Segment / Segment) { (space, externalIdentifier) =>
      val bagId = BagId(
        space = StorageSpace(space),
        externalIdentifier = ExternalIdentifier(externalIdentifier)
      )

      get {
        parameter('version.as[String] ?) { maybeVersion =>
          lookupBag(bagId = bagId, maybeVersion = maybeVersion)
        }
      }
    } ~ path(Segment / Segment / "versions") { (space, externalIdentifier) =>
      val bagId = BagId(
        space = StorageSpace(space),
        externalIdentifier = ExternalIdentifier(externalIdentifier)
      )

      get {
        parameter('before.as[String] ?) { maybeBefore =>
          lookupVersions(bagId = bagId, maybeBefore = maybeBefore)
        }
      }
    }
  }

  val bags: Route = wrapLargeResponses(routes)
}

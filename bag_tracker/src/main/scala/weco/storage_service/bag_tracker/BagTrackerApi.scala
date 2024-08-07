package weco.storage_service.bag_tracker

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import com.github.pjfanning.pekkohttpcirce.ErrorAccumulatingCirceSupport._
import grizzled.slf4j.Logging
import weco.json.JsonUtil._
import weco.storage_service.bag_tracker.services.{
  CreateBag,
  GetBag,
  GetLatestBag,
  LookupBagVersions
}
import weco.storage_service.bag_tracker.storage.StorageManifestDao
import weco.storage_service.bagit.models.{BagId, BagVersion}
import weco.storage_service.http.LookupExternalIdentifier
import weco.storage_service.storage.models.{StorageManifest, StorageSpace}
import weco.typesafe.Runnable

import scala.concurrent.{ExecutionContextExecutor, Future}

class BagTrackerApi(val storageManifestDao: StorageManifestDao)(
  host: String,
  port: Int
)(
  implicit
  actorSystem: ActorSystem
) extends Runnable
    with Logging
    with CreateBag
    with GetBag
    with GetLatestBag
    with LookupBagVersions
    with LookupExternalIdentifier {
  implicit val ec: ExecutionContextExecutor = actorSystem.dispatcher

  // Note: there are points where this API could fail if passed invalid data,
  // for example:
  //
  //    - if the path parameters are not a legal space/external identifier
  //    - if the ?before= version parameter is not an integer
  //
  // Because this is an internal-only API, we don't have nice error messages
  // if passed invalid data -- we expect apps to use the client, which should
  // enforce this sort of validation in the type system.

  val route: Route =
    pathPrefix("bags") {
      concat(
        // Store a new bag in the storage manifest dao.
        //
        // Note: we use withoutSizeLimit here to allow storing arbitraily large
        // bags using this API.  The Akka docs discourage using this function,
        // because it disables certain protections against Denial of Service attacks
        // from attackers posting large payloads -- but since this is an internal
        // API, it should be fine.
        withoutSizeLimit {
          post {
            entity(as[StorageManifest]) { storageManifest =>
              createBag(storageManifest)
            }
          }
        },
        // We look for /versions at the end of the path: this means we should
        // return a list of versions, not the complete manifest.
        //
        // The trailing slash is significant: it causes Akka to consume "/versions",
        // rather than "versions".  If you omit it, the externalIdentifier gets a
        // slash appended!
        //
        pathSuffix("versions" /) {
          path(Segment / Remaining) {
            (space, externalIdentifier) =>
              val bagId = BagId(
                space = StorageSpace(space),
                externalIdentifier =
                  decodeExternalIdentifier(externalIdentifier)
              )

              get {
                parameter('before.as[Int] ?) { maybeBefore =>
                  lookupVersions(bagId = bagId, maybeBefore = maybeBefore.map {
                    BagVersion(_)
                  })
                }
              }
          }
        },
        // Get a bag.  Optionally supplying a ?version=NNN parameter.
        get {
          pathPrefix(Segment / Remaining) {
            (space, externalIdentifier) =>
              val bagId = BagId(
                space = StorageSpace(space),
                externalIdentifier =
                  decodeExternalIdentifier(externalIdentifier)
              )

              parameter('version.as[Int] ?) {
                case None => getLatestBag(bagId = bagId)
                case Some(version) =>
                  getBag(bagId = bagId, version = BagVersion(version))
              }
          }
        }
      )
    }

  override def run(): Future[Any] = {
    for {
      server <- Http()
        .newServerAt(host, port)
        .bindFlow(route)

      _ = info(s"Listening: $host:$port")
      _ <- server.whenTerminated
      _ = info(s"Terminating: $host:$port")
    } yield server
  }
}

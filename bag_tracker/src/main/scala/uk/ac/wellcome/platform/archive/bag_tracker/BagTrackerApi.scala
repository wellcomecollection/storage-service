package uk.ac.wellcome.platform.archive.bag_tracker

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bag_tracker.services.{
  GetBag,
  LookupBagVersions
}
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagId,
  BagVersion,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.storage.models.{
  StorageManifest,
  StorageSpace
}
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContextExecutor, Future}

class BagTrackerApi(val storageManifestDao: StorageManifestDao)(
  host: String,
  port: Int
)(
  implicit
  actorSystem: ActorSystem
) extends Runnable
    with Logging
    with GetBag
    with LookupBagVersions {
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
        post {
          entity(as[StorageManifest]) { manifest =>
            println(manifest)
            println("create the manifest!")
            complete(StatusCodes.Created)
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
                externalIdentifier = ExternalIdentifier(externalIdentifier)
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
                externalIdentifier = ExternalIdentifier(externalIdentifier)
              )

              parameter('version.as[Int] ?) {
                case None =>
                  println(s"bagId = $bagId")
                  println("get the latest bag!")
                  complete(StatusCodes.OK)
                case Some(version) =>
                  getBag(bagId = bagId, version = BagVersion(version))
              }
          }
        }
      )
    }

  override def run(): Future[Any] = {
    for {
      server <- Http().bindAndHandle(
        handler = route,
        interface = host,
        port = port
      )

      _ = info(s"Listening: $host:$port")
      _ <- server.whenTerminated
      _ = info(s"Terminating: $host:$port")
    } yield server
  }
}

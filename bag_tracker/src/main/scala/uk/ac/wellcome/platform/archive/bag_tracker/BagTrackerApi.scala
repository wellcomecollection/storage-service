package uk.ac.wellcome.platform.archive.bag_tracker

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContextExecutor, Future}

class BagTrackerApi(host: String, port: Int)(
  implicit
  actorSystem: ActorSystem
) extends Runnable
    with Logging {
  implicit val ec: ExecutionContextExecutor = actorSystem.dispatcher

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
        pathSuffix("versions" /) {
          path(Segment / Remaining) { (space, remaining) =>
            println(s"space = $space")
            println(s"remaining = $remaining")
            println("get versions of it!")
            complete(StatusCodes.OK)
          }
        },
        get {
          pathPrefix(Segment / Remaining) { (space, remaining) =>
            println(s"space = $space")
            println(s"remaining = $remaining")
            println("get the bag!")
            complete(StatusCodes.OK)
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

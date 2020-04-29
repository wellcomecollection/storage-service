package uk.ac.wellcome.platform.storage.ingests_tracker

import akka.actor.ActorSystem
import uk.ac.wellcome.platform.archive.common.ingests.tracker.{IngestDoesNotExistError, IngestTracker}
import uk.ac.wellcome.typesafe.Runnable
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{get, _}
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.storage.Identified

import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import uk.ac.wellcome.json.JsonUtil._

import scala.concurrent.Future

trait IngestsTrackerApi extends Runnable with Logging {

  val ingestTracker: IngestTracker

  implicit protected val sys: ActorSystem
  implicit protected val mat: ActorMaterializer
  implicit protected val exc = sys.dispatcher

  implicit protected val host = "localhost"
  implicit protected val port = 8080

  val route: Route =
    concat(
      get {
        pathPrefix("ingest" / JavaUUID) { id =>
          ingestTracker.get(IngestID(id)) match {
            case Left(IngestDoesNotExistError(_)) =>
              complete(StatusCodes.NotFound)
            case Left(_)  =>
              complete(StatusCodes.InternalServerError)
            case Right(Identified(_, ingest)) =>
              complete(ingest)
          }
        }
      },
      get {
        pathPrefix("healthcheck") {
          complete(StatusCodes.OK)
        }
      }
    )

  override def run(): Future[Any] = {
    for {
      server <- Http().bindAndHandle(
        handler = route,
        interface = host,
        port = port
      )

      _ = info(s"Listening: ${host}:${port}")
      _ <- server.whenTerminated
      _ = info(s"Terminating: ${host}:${port}")
    } yield server
  }
}

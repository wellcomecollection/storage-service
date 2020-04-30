package uk.ac.wellcome.platform.storage.ingests_tracker

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{get, _}
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID, IngestUpdate}
import uk.ac.wellcome.platform.archive.common.ingests.tracker._
import uk.ac.wellcome.storage.Identified
import uk.ac.wellcome.typesafe.Runnable

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
      post {
        entity(as[Ingest]) { ingest =>
          ingestTracker.init(ingest) match {
            case Right(_) =>
              info(s"Created ingest: ${ingest}")
              complete(StatusCodes.Created)
            case Left(e: StateConflictError) =>
              error(s"Ingest could not be created: ${ingest.id}", e)
              complete(StatusCodes.Conflict)
            case Left(e) =>
              error("Failed to create ingest!", e)
              complete(StatusCodes.InternalServerError)
          }
        }
      },
      patch {
        pathPrefix("ingest" / JavaUUID) { id =>
          entity(as[IngestUpdate]) { ingestUpdate =>
            info(s"Updating $id: $ingestUpdate")

            ingestTracker.update(ingestUpdate) match {
              case Left(e: StateConflictError) =>
                error(s"Ingest ${id} can not be updated", e)
                complete(StatusCodes.Conflict)
              case Left(e) =>
                error(s"Failed to update ingest: ${id}", e)
                complete(StatusCodes.InternalServerError)
              case Right(Identified(_,ingest)) =>
                info(s"Updated ingest: $ingest")
                complete(StatusCodes.OK -> ingest)
            }
          }
        }
      },
      get {
        pathPrefix("ingest" / JavaUUID) { id =>
          ingestTracker.get(IngestID(id)) match {
            case Left(IngestDoesNotExistError(_)) =>
              info(s"Could not find ingest: ${id}")
              complete(StatusCodes.NotFound)
            case Left(e) =>
              error("Failed to get ingest!", e)
              complete(StatusCodes.InternalServerError)
            case Right(Identified(_, ingest)) =>
              info(s"Retrieved ingest: ${ingest}")
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

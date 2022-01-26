package weco.storage_service.ingests_tracker

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{get, _}
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import grizzled.slf4j.Logging
import weco.json.JsonUtil._
import weco.storage_service.ingests.models.{Ingest, IngestID, IngestUpdate}
import weco.storage_service.ingests_tracker.services.MessagingService
import weco.storage_service.ingests_tracker.tracker.{
  IngestDoesNotExistError,
  IngestTracker,
  StateConflictError,
  UpdateNonExistentIngestError
}
import weco.storage.Identified
import weco.typesafe.Runnable

import scala.concurrent.{ExecutionContextExecutor, Future}

class IngestsTrackerApi[CallbackDestination, IngestsDestination](
  ingestTracker: IngestTracker,
  messagingService: MessagingService[CallbackDestination, IngestsDestination]
)(
  host: String = "localhost",
  port: Int = 8080
)(
  implicit sys: ActorSystem
) extends Runnable
    with Logging {

  implicit val exc: ExecutionContextExecutor = sys.dispatcher

  val route: Route =
    concat(
      post {
        pathPrefix("ingest") {
          entity(as[Ingest]) {
            ingest =>
              ingestTracker.init(ingest) match {
                case Right(_) =>
                  info(s"Created ingest: $ingest")
                  messagingService.send(ingest)
                  complete(StatusCodes.Created)
                case Left(e: StateConflictError) =>
                  error(s"Ingest could not be created: ${ingest.id}", e)
                  complete(StatusCodes.Conflict)
                case Left(e) =>
                  error("Failed to create ingest!", e)
                  complete(StatusCodes.InternalServerError)
              }
          }
        }
      },
      patch {
        pathPrefix("ingest" / JavaUUID) {
          id =>
            entity(as[IngestUpdate]) {
              ingestUpdate =>
                info(s"Updating $id: $ingestUpdate")

                ingestTracker.update(ingestUpdate) match {
                  case Right(Identified(_, ingest)) =>
                    info(s"Updated ingest: $ingest")
                    messagingService.send(ingest)
                    complete(StatusCodes.OK -> ingest)
                  case Left(UpdateNonExistentIngestError(e)) =>
                    error(s"Could not find ingest $id to update: $e")
                    complete(StatusCodes.NotFound)
                  case Left(e: StateConflictError) =>
                    error(s"Ingest $id can not be updated", e)
                    complete(StatusCodes.Conflict)
                  case Left(e) =>
                    error(s"Failed to update ingest: $id", e)
                    complete(StatusCodes.InternalServerError)
                }
            }
        }
      },
      get {
        pathPrefix("ingest" / JavaUUID) {
          id =>
            ingestTracker.get(IngestID(id)) match {
              case Left(IngestDoesNotExistError(_)) =>
                info(s"Could not find ingest: $id")
                complete(StatusCodes.NotFound)
              case Left(e) =>
                error("Failed to get ingest!", e)
                complete(StatusCodes.InternalServerError)
              case Right(Identified(_, ingest)) =>
                info(s"Retrieved ingest: $ingest")
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
      server <- Http()
        .newServerAt(host, port)
        .bindFlow(route)

      _ = info(s"Listening: $host:$port")
      _ <- server.whenTerminated
      _ = info(s"Terminating: $host:$port")
    } yield server
  }
}

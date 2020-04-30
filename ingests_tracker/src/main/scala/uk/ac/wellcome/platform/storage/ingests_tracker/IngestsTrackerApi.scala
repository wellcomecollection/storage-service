package uk.ac.wellcome.platform.storage.ingests_tracker

import akka.actor.ActorSystem
import uk.ac.wellcome.platform.archive.common.ingests.tracker.{
  IngestAlreadyExistsError,
  IngestDoesNotExistError,
  IngestTracker
}
import uk.ac.wellcome.typesafe.Runnable
import akka.http.scaladsl.model._
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.storage.Identified
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import uk.ac.wellcome.json.JsonUtil._

import scala.concurrent.{ExecutionContextExecutor, Future}

trait IngestsTrackerApi extends Runnable with Logging {

  val ingestTracker: IngestTracker

  implicit protected val sys: ActorSystem
  implicit protected val mat: Materializer
  implicit protected val exc: ExecutionContextExecutor = sys.dispatcher

  implicit protected val host: String = "localhost"
  implicit protected val port: Int = 8080

  val route: Route =
    concat(
      post {
        entity(as[Ingest]) {
          ingest =>
            ingestTracker.init(ingest) match {
              case Right(_) =>
                info(s"Created ingest: ${ingest}")
                complete(StatusCodes.Created)
              case Left(e @ IngestAlreadyExistsError(_)) =>
                error(s"Ingest already exists: ${ingest.id}", e)
                complete(StatusCodes.Conflict)
              case Left(e) =>
                error("Failed to create ingest!", e)
                complete(StatusCodes.InternalServerError)
            }
        }
      },
      get {
        pathPrefix("ingest" / JavaUUID) {
          id =>
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

      _ = info(s"Listening: $host:$port")
      _ <- server.whenTerminated
      _ = info(s"Terminating: $host:$port")
    } yield server
  }
}

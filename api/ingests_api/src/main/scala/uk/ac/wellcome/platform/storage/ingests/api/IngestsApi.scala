package uk.ac.wellcome.platform.storage.ingests.api

import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server._
import akka.http.scaladsl.server.Directives._
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.http.models.InternalServerErrorResponse
import uk.ac.wellcome.platform.archive.display.ingests.RequestDisplayIngest
import uk.ac.wellcome.platform.storage.ingests.api.responses.{
  CreateIngest,
  LookupIngest
}

import scala.concurrent.Future
import scala.util.{Failure, Success}

trait IngestsApi extends CreateIngest with LookupIngest {
  val ingests: Route = pathPrefix("ingests") {
    post {
      entity(as[RequestDisplayIngest]) { createIngest }
    } ~ path(JavaUUID) { id: UUID =>
      get {
        withFuture { lookupIngest(id) }
      }
    }
  }

  private def withFuture(future: Future[Route]): Route =
    onComplete(future) {
      case Success(resp) => resp
      case Failure(_) =>
        complete(
          StatusCodes.InternalServerError -> InternalServerErrorResponse(
            contextURL,
            statusCode = StatusCodes.InternalServerError
          )
        )
    }
}

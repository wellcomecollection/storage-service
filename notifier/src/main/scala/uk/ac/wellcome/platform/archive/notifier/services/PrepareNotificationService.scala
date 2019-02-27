package uk.ac.wellcome.platform.archive.notifier.services

import java.util.UUID

import akka.http.scaladsl.model.HttpResponse
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.progress.models.Callback.{
  Failed,
  Succeeded
}
import uk.ac.wellcome.platform.archive.common.progress.models.ProgressCallbackStatusUpdate

import scala.util.{Failure, Success, Try}

object PrepareNotificationService extends Logging {
  def prepare(id: UUID,
              httpResponse: Try[HttpResponse]): ProgressCallbackStatusUpdate =
    httpResponse match {
      case Success(HttpResponse(status, _, _, _)) =>
        if (status.isSuccess()) {
          debug(s"Callback fulfilled for: $id")

          ProgressCallbackStatusUpdate(
            id = id,
            callbackStatus = Succeeded,
            description = "Callback fulfilled."
          )
        } else {
          debug(s"Callback failed for: $id, got $status!")

          ProgressCallbackStatusUpdate(
            id = id,
            callbackStatus = Failed,
            description = s"Callback failed for: $id, got $status!"
          )
        }
      case Failure(e) =>
        error(s"Callback failed for: $id", e)

        ProgressCallbackStatusUpdate(
          id = id,
          callbackStatus = Failed,
          description = s"Callback failed for: $id (${e.getMessage})"
        )
    }
}

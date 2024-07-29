package weco.storage_service.notifier.services

import org.apache.pekko.http.scaladsl.model.HttpResponse
import grizzled.slf4j.Logging
import weco.storage_service.ingests.models.Callback.{Failed, Succeeded}
import weco.storage_service.ingests.models.IngestCallbackStatusUpdate
import weco.storage_service.ingests.models.{
  IngestCallbackStatusUpdate,
  IngestID
}

import scala.util.{Failure, Success, Try}

object PrepareNotificationService extends Logging {
  def prepare(
    id: IngestID,
    httpResponse: Try[HttpResponse]
  ): IngestCallbackStatusUpdate =
    httpResponse match {
      case Success(HttpResponse(status, _, _, _)) =>
        if (status.isSuccess()) {
          debug(s"Callback fulfilled for: $id")

          IngestCallbackStatusUpdate(
            id = id,
            callbackStatus = Succeeded,
            description = "Callback fulfilled"
          )
        } else {
          debug(s"Callback failed for: $id, got $status!")

          IngestCallbackStatusUpdate(
            id = id,
            callbackStatus = Failed,
            description = s"Callback failed for: $id, got $status!"
          )
        }
      case Failure(e) =>
        error(s"Callback failed for: $id", e)

        IngestCallbackStatusUpdate(
          id = id,
          callbackStatus = Failed,
          description = s"Callback failed for: $id (${e.getMessage})"
        )
    }
}

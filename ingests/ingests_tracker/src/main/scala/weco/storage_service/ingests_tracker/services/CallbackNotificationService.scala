package weco.storage_service.ingests_tracker.services

import java.net.URI

import grizzled.slf4j.Logging
import weco.json.JsonUtil._
import weco.messaging.MessageSender
import weco.storage_service.ingests.models.Callback.Pending
import weco.storage_service.ingests.models.{
  Callback,
  CallbackNotification,
  Ingest
}

import scala.util.{Success, Try}

class CallbackNotificationService[Destination](
  messageSender: MessageSender[Destination]
) extends Logging {

  def sendNotification(ingest: Ingest): Try[Unit] =
    (ingest.status, ingest.callback) match {
      case (_: Ingest.Completed, Some(Callback(uri, Pending))) =>
        sendSnsMessage(uri, ingest)
      case _ => Success(())
    }

  private def sendSnsMessage(callbackUri: URI, ingest: Ingest): Try[Unit] = {
    val callbackNotification = CallbackNotification(
      ingestId = ingest.id,
      callbackUri = callbackUri,
      payload = ingest
    )

    messageSender.sendT(callbackNotification)
  }
}

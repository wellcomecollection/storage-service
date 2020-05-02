package uk.ac.wellcome.platform.storage.ingests_tracker.services

import java.net.URI

import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.MessageSender
import uk.ac.wellcome.platform.archive.common.ingests.models.Callback.Pending
import uk.ac.wellcome.platform.archive.common.ingests.models.{
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

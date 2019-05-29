package uk.ac.wellcome.platform.archive.ingests.services

import java.net.URI

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
  messageSender: MessageSender[Destination]) {
  def sendNotification(ingest: Ingest): Try[Unit] =
    ingest.callback match {
      case Some(Callback(callbackUri, Pending)) =>
        ingest.status match {
          case Ingest.Completed | Ingest.Failed =>
            sendSnsMessage(callbackUri, ingest = ingest)
          case _ => Success(())
        }
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

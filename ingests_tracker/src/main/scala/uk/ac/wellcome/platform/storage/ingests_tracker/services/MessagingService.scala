package uk.ac.wellcome.platform.storage.ingests_tracker.services

import grizzled.slf4j.Logging
import uk.ac.wellcome.messaging.MessageSender
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest

import uk.ac.wellcome.json.JsonUtil._

import scala.util.{Failure, Success, Try}

class MessagingService[UpdatedIngestsDestination, CallbackDestination](
  callbackNotificationService: CallbackNotificationService[CallbackDestination],
  updatedIngestsMessageSender: MessageSender[UpdatedIngestsDestination]
) extends Logging {

  def sendOngoingMessages(ingest: Ingest): Try[Unit] = {
    val callbackResult = callbackNotificationService.sendNotification(ingest)
    val updatedIngestResult = updatedIngestsMessageSender.sendT(ingest)

    (callbackResult, updatedIngestResult) match {
      case (Success(_), Success(_)) => Success(())

      case (Failure(callbackErr), Success(_)) =>
        warn(s"Failed to send the callback notification: $callbackErr")
        Failure(callbackErr)

      case (Success(_), Failure(updatedIngestErr)) =>
        warn(s"Failed to send the updated ingest: $updatedIngestErr")
        Failure(updatedIngestErr)

      case (Failure(callbackErr), Failure(updatedIngestErr)) =>
        warn(s"Failed to send the callback notification: $callbackErr")
        warn(s"Failed to send the updated ingest: $updatedIngestErr")
        Failure(
          new Throwable(
            "Both of the ongoing messages failed to send correctly!"
          )
        )
    }
  }
}

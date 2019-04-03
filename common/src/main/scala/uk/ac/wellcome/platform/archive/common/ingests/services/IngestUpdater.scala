package uk.ac.wellcome.platform.archive.common.ingests.services

import java.util.UUID

import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.{PublishAttempt, SNSWriter}
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestEvent, IngestStatusUpdate, IngestUpdate}
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestCompleted, IngestFailed, IngestStepResult, IngestStepSuccess}

import scala.concurrent.Future

class IngestUpdater(
  stepName: String,
  snsWriter: SNSWriter
) {

  def send[R](
    requestId: UUID,
    result: IngestStepResult[R],
    bagId: Option[BagId] = None
  ): Future[PublishAttempt] = {
    val update = result match {
      case IngestCompleted(_) =>
        IngestStatusUpdate(
          id = requestId,
          status = Ingest.Completed,
          affectedBag = bagId,
          events = List(
            IngestEvent(
              s"${stepName.capitalize} succeeded (completed)"
            )
          )
        )

      case IngestStepSuccess(_) =>
        IngestUpdate.event(
          id = requestId,
          description = s"${stepName.capitalize} succeeded"
        )

      case IngestFailed(_, _, maybeMessage) =>
        IngestStatusUpdate(
          id = requestId,
          status = Ingest.Failed,
          affectedBag = bagId,
          events = List(
            IngestEvent(
              eventDescription(s"${stepName.capitalize} failed", maybeMessage)
            )
          )
        )
    }

    snsWriter.writeMessage[IngestUpdate](
      update,
      subject = s"Sent by ${this.getClass.getSimpleName}"
    )
  }

  val descriptionMaxLength = 250
  private def eventDescription(main:String, maybeInformation:Option[String]): String = {
    val separator: String = " - "
    truncate(
      Seq(
        Some(main), maybeInformation
      ).flatten.mkString(separator),
      descriptionMaxLength)
  }

  private def truncate(text: String, maxLength:Int): String = {
    if (text.length > maxLength) {
      val truncatedText = text.take(maxLength).trim
      if (truncatedText.length == maxLength && maxLength > 3) {
        truncatedText.dropRight(3).concat("...")
      } else {
        truncatedText
      }
    } else {
      text
    }
  }
}

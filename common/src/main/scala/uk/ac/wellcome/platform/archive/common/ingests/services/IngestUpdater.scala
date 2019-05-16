package uk.ac.wellcome.platform.archive.common.ingests.services

import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.SNSWriter
import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.platform.archive.common.storage.models._

import scala.concurrent.{ExecutionContext, Future}

class IngestUpdater(
  stepName: String,
  snsWriter: SNSWriter
)(implicit ec: ExecutionContext)
    extends Logging {

  def start(ingestId: IngestID): Future[Unit] =
    send(
      ingestId = ingestId,
      step = IngestStepStarted(ingestId)
    )

  def send[R](
    ingestId: IngestID,
    step: IngestStep[R],
    bagId: Option[BagId] = None
  ): Future[Unit] = {
    val update = step match {
      case IngestCompleted(_) =>
        IngestStatusUpdate(
          id = ingestId,
          status = Ingest.Completed,
          affectedBag = bagId,
          events = List(
            IngestEvent(
              s"${stepName.capitalize} succeeded (completed)"
            )
          )
        )

      case IngestStepSucceeded(_) =>
        IngestUpdate.event(
          id = ingestId,
          description = s"${stepName.capitalize} succeeded"
        )

      case IngestStepStarted(_) =>
        IngestUpdate.event(
          id = ingestId,
          description = s"${stepName.capitalize} started"
        )

      case IngestFailed(_, _, maybeMessage) =>
        IngestStatusUpdate(
          id = ingestId,
          status = Ingest.Failed,
          affectedBag = bagId,
          events = List(
            IngestEvent(
              eventDescription(s"${stepName.capitalize} failed", maybeMessage)
            )
          )
        )
    }

    snsWriter
      .writeMessage[IngestUpdate](
        update,
        subject = s"Sent by ${this.getClass.getSimpleName}"
      )
      .map { _ =>
        ()
      }
  }

  def sendEvent(ingestId: IngestID, messages: Seq[String]): Future[Unit] = {
    val update: IngestUpdate = IngestEventUpdate(
      id = ingestId,
      events = messages.map { m: String =>
        IngestEvent(eventDescription(m))
      }
    )

    snsWriter
      .writeMessage[IngestUpdate](
        update,
        subject = s"Sent by ${this.getClass.getSimpleName}"
      )
      .map { _ =>
        ()
      }
  }

  val descriptionMaxLength = 250
  private def eventDescription(
    main: String,
    maybeInformation: Option[String] = None): String = {
    val separator: String = " - "
    truncate(
      Seq(
        Some(main),
        maybeInformation
      ).flatten.mkString(separator),
      descriptionMaxLength)
  }

  private def truncate(text: String, maxLength: Int): String = {
    if (text.length > maxLength) {
      val truncatedText = text.take(maxLength).trim
      if (truncatedText.length == maxLength && maxLength > 3) {
        warn(
          s"Truncated message, too long to send as an ingest progress message (>$maxLength)")
        truncatedText.dropRight(3).concat("...")
      } else {
        truncatedText
      }
    } else {
      text
    }
  }
}

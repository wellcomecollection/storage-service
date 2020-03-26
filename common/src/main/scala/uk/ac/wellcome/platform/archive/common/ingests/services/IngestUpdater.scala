package uk.ac.wellcome.platform.archive.common.ingests.services

import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.MessageSender
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.platform.archive.common.storage.models._

import scala.util.Try

class IngestUpdater[Destination](
  val stepName: String,
  messageSender: MessageSender[Destination]
) extends Logging {

  def start(ingestId: IngestID): Try[Unit] =
    send(
      ingestId = ingestId,
      step = IngestStepStarted(ingestId)
    )

  def send[R](
    ingestId: IngestID,
    step: IngestStep[R]
  ): Try[Unit] = {
    debug(s"Sending an ingest update for ID=$ingestId step=$step")
    val update = step match {
      case IngestCompleted(_) =>
        IngestStatusUpdate(
          id = ingestId,
          status = Ingest.Completed,
          events = List(
            IngestEvent(
              s"${stepName.capitalize} succeeded (completed)"
            )
          )
        )

      case IngestStepSucceeded(_, maybeMessage) =>
        IngestEventUpdate(
          id = ingestId,
          events = Seq(
            IngestEvent(
              description = eventDescription(
                s"${stepName.capitalize} succeeded",
                maybeMessage
              )
            )
          )
        )

      case IngestStepStarted(_) =>
        IngestEventUpdate(
          id = ingestId,
          events = Seq(
            IngestEvent(
              description = s"${stepName.capitalize} started"
            )
          )
        )

      case IngestShouldRetry(_, _, maybeMessage) =>
        IngestEventUpdate(
          id = ingestId,
          events = Seq(
            IngestEvent(
              description = eventDescription(
                s"${stepName.capitalize} retrying",
                maybeMessage
              )
            )
          )
        )

      case IngestFailed(_, _, maybeMessage) =>
        IngestStatusUpdate(
          id = ingestId,
          status = Ingest.Failed,
          events = List(
            IngestEvent(
              eventDescription(s"${stepName.capitalize} failed", maybeMessage)
            )
          )
        )
    }

    sendUpdate(update)
  }

  def sendUpdate(update: IngestUpdate): Try[Unit] =
    messageSender.sendT[IngestUpdate](update)

  val descriptionMaxLength = 250
  private def eventDescription(
    main: String,
    maybeInformation: Option[String] = None
  ): String = {
    val separator: String = " - "
    truncate(
      Seq(
        Some(main),
        maybeInformation
      ).flatten.mkString(separator),
      descriptionMaxLength
    )
  }

  private def truncate(text: String, maxLength: Int): String = {
    if (text.length > maxLength) {
      val truncatedText = text.take(maxLength).trim
      if (truncatedText.length == maxLength && maxLength > 3) {
        warn(
          s"Truncated message, too long to send as an ingest progress message (>$maxLength)"
        )
        truncatedText.dropRight(3).concat("...")
      } else {
        truncatedText
      }
    } else {
      text
    }
  }
}

package uk.ac.wellcome.platform.archive.common.ingests.models

import java.util.UUID

case class ProgressNotice(
  id: UUID,
  message: String,
  status: Option[Ingest.Status]
) {
  def toUpdate() = {
    val event = ProgressEvent(message)

    status match {
      case Some(status) =>
        ProgressStatusUpdate(
          id = id,
          status = status,
          affectedBag = None,
          events = List(event)
        )

      case None =>
        ProgressEventUpdate(
          id = id,
          events = List(event)
        )
    }
  }
}

object ProgressNotice {
  def apply(id: UUID, message: String*): ProgressNotice = {
    ProgressNotice(
      id,
      message.mkString(" "),
      None
    )
  }

  def apply(id: UUID,
            status: Ingest.Status,
            message: String*): ProgressNotice = {
    ProgressNotice(
      id,
      message.mkString(" "),
      Some(status)
    )
  }
}

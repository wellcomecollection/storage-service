package uk.ac.wellcome.platform.archive.common.ingests.models

import java.util.UUID

case class IngestNotice(
  id: UUID,
  message: String,
  status: Option[Ingest.Status]
) {
  def toUpdate() = {
    val event = IngestEvent(message)

    status match {
      case Some(status) =>
        IngestStatusUpdate(
          id = id,
          status = status,
          affectedBag = None,
          events = List(event)
        )

      case None =>
        IngestEventUpdate(
          id = id,
          events = List(event)
        )
    }
  }
}

object IngestNotice {
  def apply(id: UUID, message: String*): IngestNotice = {
    IngestNotice(
      id,
      message.mkString(" "),
      None
    )
  }

  def apply(id: UUID,
            status: Ingest.Status,
            message: String*): IngestNotice = {
    IngestNotice(
      id,
      message.mkString(" "),
      Some(status)
    )
  }
}

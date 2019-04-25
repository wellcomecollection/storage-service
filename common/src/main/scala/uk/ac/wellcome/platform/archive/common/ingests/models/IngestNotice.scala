package uk.ac.wellcome.platform.archive.common.ingests.models

import uk.ac.wellcome.platform.archive.common.IngestID

case class IngestNotice(
  id: IngestID,
  message: String,
  status: Option[Ingest.Status]
) {
  def toUpdate(): IngestUpdate = {
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
  def apply(id: IngestID, message: String*): IngestNotice = {
    IngestNotice(
      id = id,
      message = message.mkString(" "),
      status = None
    )
  }

  def apply(id: IngestID,
            status: Ingest.Status,
            message: String*): IngestNotice = {
    IngestNotice(
      id = id,
      message = message.mkString(" "),
      status = Some(status)
    )
  }
}

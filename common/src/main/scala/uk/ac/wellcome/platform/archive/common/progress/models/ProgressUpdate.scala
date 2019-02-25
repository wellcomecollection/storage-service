package uk.ac.wellcome.platform.archive.common.progress.models

import java.util.UUID

import uk.ac.wellcome.platform.archive.common.models.bagit.BagId
import uk.ac.wellcome.platform.archive.common.models.error.ArchiveError

sealed trait ProgressUpdate {
  val id: UUID
  val events: Seq[ProgressEvent]
}

case class ProgressEventUpdate(id: UUID, events: Seq[ProgressEvent])
    extends ProgressUpdate

case object ProgressEventUpdate {
  def apply(id: UUID, description: String): ProgressEventUpdate =
    ProgressEventUpdate(id = id, events = Seq(ProgressEvent(description)))
}

case class ProgressStatusUpdate(
  id: UUID,
  status: Progress.Status,
  affectedBag: Option[BagId],
  events: Seq[ProgressEvent]
) extends ProgressUpdate

case object ProgressStatusUpdate {
  def apply(id: UUID, status: Progress.Status, affectedBag: BagId, events: Seq[String] = List.empty): ProgressStatusUpdate =
    ProgressStatusUpdate(
      id = id,
      status = status,
      affectedBag = Some(affectedBag),
      events = events.map { ProgressEvent(_) }
    )

  def apply(id: UUID, status: Progress.Status, events: Seq[String] = List.empty): ProgressStatusUpdate =
    ProgressStatusUpdate(
      id = id,
      status = status,
      affectedBag = None,
      events = events.map { ProgressEvent(_) }
    )
}

case class ProgressCallbackStatusUpdate(
  id: UUID,
  callbackStatus: Callback.CallbackStatus,
  events: Seq[ProgressEvent]
) extends ProgressUpdate

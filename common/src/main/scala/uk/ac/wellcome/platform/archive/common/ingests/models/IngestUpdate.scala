package uk.ac.wellcome.platform.archive.common.ingests.models

import java.util.UUID

import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.bagit.models.error.ArchiveError

sealed trait IngestUpdate {
  val id: UUID
  val events: Seq[IngestEvent]
}
object IngestUpdate {

  def failed[T](id: UUID, error: ArchiveError[T]) =
    IngestStatusUpdate(
      id,
      Ingest.Failed,
      None,
      List(IngestEvent(error.toString))
    )

  def event(id: UUID, description: String) =
    IngestEventUpdate(id, Seq(IngestEvent(description)))

}

case class IngestEventUpdate(id: UUID, events: Seq[IngestEvent])
    extends IngestUpdate

case class IngestStatusUpdate(id: UUID,
                              status: Ingest.Status,
                              affectedBag: Option[BagId],
                              events: Seq[IngestEvent] = List.empty)
    extends IngestUpdate

case class IngestCallbackStatusUpdate(
  id: UUID,
  callbackStatus: Callback.CallbackStatus,
  events: Seq[IngestEvent]
) extends IngestUpdate

case object IngestCallbackStatusUpdate {
  def apply(id: UUID,
            callbackStatus: Callback.CallbackStatus,
            description: String): IngestCallbackStatusUpdate =
    IngestCallbackStatusUpdate(
      id = id,
      callbackStatus = callbackStatus,
      events = List(IngestEvent(description))
    )
}

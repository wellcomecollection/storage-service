package uk.ac.wellcome.platform.archive.common.ingests.models

import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.bagit.models.error.ArchiveError

sealed trait IngestUpdate {
  val id: IngestID
  val events: Seq[IngestEvent]
}

object IngestUpdate {

  def failed[T](id: IngestID, error: ArchiveError[T]) =
    IngestStatusUpdate(
      id = id,
      status = Ingest.Failed,
      affectedBag = None,
      events = List(IngestEvent(error.toString))
    )

  def event(id: IngestID, description: String) =
    IngestEventUpdate(id, Seq(IngestEvent(description)))

}

case class IngestEventUpdate(id: IngestID, events: Seq[IngestEvent])
    extends IngestUpdate

case class IngestStatusUpdate(id: IngestID,
                              status: Ingest.Status,
                              affectedBag: Option[BagId],
                              events: Seq[IngestEvent] = List.empty)
    extends IngestUpdate

case class IngestCallbackStatusUpdate(
  id: IngestID,
  callbackStatus: Callback.CallbackStatus,
  events: Seq[IngestEvent]
) extends IngestUpdate

case object IngestCallbackStatusUpdate {
  def apply(id: IngestID,
            callbackStatus: Callback.CallbackStatus,
            description: String): IngestCallbackStatusUpdate =
    IngestCallbackStatusUpdate(
      id = id,
      callbackStatus = callbackStatus,
      events = List(IngestEvent(description))
    )
}

package uk.ac.wellcome.platform.archive.common.ingests.models

sealed trait IngestUpdate {
  val id: IngestID
  val events: Seq[IngestEvent]
}

case class IngestEventUpdate(id: IngestID, events: Seq[IngestEvent])
    extends IngestUpdate

case class IngestStatusUpdate(id: IngestID,
                              status: Ingest.Status,
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

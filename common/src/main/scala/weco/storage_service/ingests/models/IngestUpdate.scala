package weco.storage_service.ingests.models

import weco.storage_service.bagit.models.BagVersion

sealed trait IngestUpdate {
  val id: IngestID
  val events: Seq[IngestEvent]
}

case class IngestEventUpdate(
  id: IngestID,
  events: Seq[IngestEvent]
) extends IngestUpdate

case class IngestStatusUpdate(
  id: IngestID,
  status: Ingest.Status,
  events: Seq[IngestEvent] = Seq.empty
) extends IngestUpdate

case class IngestCallbackStatusUpdate(
  id: IngestID,
  callbackStatus: Callback.CallbackStatus,
  events: Seq[IngestEvent]
) extends IngestUpdate

case object IngestCallbackStatusUpdate {
  def apply(
    id: IngestID,
    callbackStatus: Callback.CallbackStatus,
    description: String
  ): IngestCallbackStatusUpdate =
    IngestCallbackStatusUpdate(
      id = id,
      callbackStatus = callbackStatus,
      events = List(IngestEvent(description))
    )
}

case class IngestVersionUpdate(
  id: IngestID,
  events: Seq[IngestEvent],
  version: BagVersion
) extends IngestUpdate

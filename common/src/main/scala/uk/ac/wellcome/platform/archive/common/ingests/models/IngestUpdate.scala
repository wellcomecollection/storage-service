package uk.ac.wellcome.platform.archive.common.ingests.models

import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion

sealed trait IngestUpdate {
  val id: IngestID
  val events: Seq[IngestEvent]
  val `type`: String
}

case class IngestEventUpdate(
  id: IngestID,
  events: Seq[IngestEvent],
  `type`: String = "IngestEventUpdate"
) extends IngestUpdate

case class IngestStatusUpdate(
  id: IngestID,
  status: Ingest.Status,
  events: Seq[IngestEvent] = Seq.empty,
  `type`: String = "IngestStatusUpdate"
) extends IngestUpdate

case class IngestCallbackStatusUpdate(
  id: IngestID,
  callbackStatus: Callback.CallbackStatus,
  events: Seq[IngestEvent],
  `type`: String = "IngestCallbackStatusUpdate"
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
  version: BagVersion,
  `type`: String = "IngestVersionUpdate"
) extends IngestUpdate
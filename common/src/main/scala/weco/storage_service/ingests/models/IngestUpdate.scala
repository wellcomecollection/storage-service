package weco.storage_service.ingests.models

import weco.storage_service.bagit.models.BagVersion

/** These are the messages sent to the `ingests_tracker` by the individual
  * worker services.  They send a message asynchronously whenever they start
  * or stop a processing step, and the tracker updates the ingests database.
  *
  *         worker1 ---+
  *                    |
  *         worker2 ---+---> ingests tracker ---> ingests database
  *                    |
  *         worker3 ---+
  *
  */
sealed trait IngestUpdate {
  val id: IngestID
  val events: Seq[IngestEvent]
}

/** This is the most general update, that just records "some processing happened".
  *
  * This is sent by services that aren't using one of the more specific update types.
  *
  */
case class IngestEventUpdate(
  id: IngestID,
  events: Seq[IngestEvent]
) extends IngestUpdate

/** This records a change in the status of the ingest.  This can be sent in two ways:
  *
  *     1)  when the bag register successfully stores a bag, it sends a "success"
  *         status update so the ingests tracker can mark the ingest as succeeded
  *
  *     2)  if there's an error while processing, any worker can send a "failed" update
  *
  */
case class IngestStatusUpdate(
  id: IngestID,
  status: Ingest.Status,
  events: Seq[IngestEvent] = Seq.empty
) extends IngestUpdate

/** This is sent by the notifier when it completes a callback.
  *
  * It includes the status of the callback, which is separate from the overall status
  * of the ingest -- e.g. a bag may store successfully but we can't tell the original
  * workflow system for some reason.
  *
  */
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

/** This is sent by the bag versioner when it assigns a version to the bag.
  *
  * The version will be recorded as part of the ingest.
  *
  */
case class IngestVersionUpdate(
  id: IngestID,
  events: Seq[IngestEvent],
  version: BagVersion
) extends IngestUpdate

package uk.ac.wellcome.platform.archive.common.ingests.services

import uk.ac.wellcome.platform.archive.common.ingests.models._

import scala.util.Try

class IngestStatusGoingBackwardsException(val existing: Ingest.Status,
                                          val update: Ingest.Status)
    extends RuntimeException(
      s"Received status update $update, but ingest already has status $existing")

class CallbackStatusGoingBackwardsException(
  val existing: Callback.CallbackStatus,
  val update: Callback.CallbackStatus)
    extends RuntimeException(
      s"Received callback status update $update, but ingest already has status $existing")

class NoCallbackException
    extends RuntimeException(
      "Received callback status update, but ingest doesn't have a callback")

object IngestStates {
  def applyUpdate(ingest: Ingest, update: IngestUpdate): Try[Ingest] = Try {
    update match {
      case _: IngestEventUpdate =>
        // Update the ingest status to "processing" when we see the first
        // event update
        val updatedStatus = ingest.status match {
          case Ingest.Accepted => Ingest.Processing
          case _               => ingest.status
        }

        ingest.copy(
          status = updatedStatus,
          events = ingest.events ++ update.events
        )

      case statusUpdate: IngestStatusUpdate =>
        if (!statusUpdateIsAllowed(ingest.status, statusUpdate.status)) {
          throw new IngestStatusGoingBackwardsException(
            ingest.status,
            statusUpdate.status)
        }

        ingest.copy(
          status = statusUpdate.status,
          events = ingest.events ++ update.events
        )

      case callbackStatusUpdate: IngestCallbackStatusUpdate =>
        ingest.callback match {
          case Some(callback) =>
            if (!callbackStatusUpdateIsAllowed(
                  callback.status,
                  callbackStatusUpdate.callbackStatus)) {
              throw new CallbackStatusGoingBackwardsException(
                callback.status,
                callbackStatusUpdate.callbackStatus
              )
            }

            ingest.copy(
              callback = Some(
                callback.copy(status = callbackStatusUpdate.callbackStatus)),
              events = ingest.events ++ update.events
            )

          case None => throw new NoCallbackException()
        }
    }
  }

  private def statusUpdateIsAllowed(initial: Ingest.Status,
                                    update: Ingest.Status): Boolean =
    initial match {
      case status if status == update => true

      case Ingest.Accepted   => true
      case Ingest.Processing => update != Ingest.Accepted
      case Ingest.Completed  => false
      case Ingest.Failed     => false
    }

  private def callbackStatusUpdateIsAllowed(
    initial: Callback.CallbackStatus,
    update: Callback.CallbackStatus): Boolean =
    initial match {
      case status if status == update => true

      case Callback.Pending   => true
      case Callback.Succeeded => false
      case Callback.Failed    => false
    }
}

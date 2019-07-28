package uk.ac.wellcome.platform.archive.common.ingests.services

import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
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

class MismatchedVersionUpdateException(val existing: BagVersion,
                                       val update: BagVersion)
    extends RuntimeException(
      s"Received bag version update $update, but ingest already has version $existing")

object IngestStates {
  def applyUpdate(ingest: Ingest, update: IngestUpdate): Try[Ingest] = Try {
    val newEvents = ingest.events ++ update.events

    val newIngest = ingest.copy(
      events = newEvents
    )

    update match {
      case _: IngestEventUpdate =>
        // Update the ingest status to "processing" when we see the first
        // event update
        val updatedStatus = ingest.status match {
          case Ingest.Accepted => Ingest.Processing
          case _               => ingest.status
        }

        newIngest.copy(
          status = updatedStatus
        )

      case statusUpdate: IngestStatusUpdate =>
        if (!statusUpdateIsAllowed(ingest.status, statusUpdate.status)) {
          throw new IngestStatusGoingBackwardsException(
            ingest.status,
            statusUpdate.status)
        }

        newIngest.copy(
          status = statusUpdate.status
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

            newIngest.copy(
              callback = Some(
                callback.copy(status = callbackStatusUpdate.callbackStatus))
            )

          case None => throw new NoCallbackException()
        }

      case IngestVersionUpdate(_, _, updateVersion) =>
        ingest.version match {
          case None =>
            newIngest.copy(
              version = Some(updateVersion)
            )

          case Some(version) if version == updateVersion =>
            newIngest

          case Some(version) =>
            throw new MismatchedVersionUpdateException(version, updateVersion)
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

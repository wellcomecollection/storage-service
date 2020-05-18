package uk.ac.wellcome.platform.storage.ingests_tracker.tracker

import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
import uk.ac.wellcome.platform.archive.common.ingests.models._

object IngestStates {
  def applyUpdate(
    ingest: Ingest,
    update: IngestUpdate
  ): Either[IngestStoreError, Ingest] = {

    val newEvents =
      ingest.events ++ update.events

    val newIngest =
      ingest.copy(
        events = newEvents
      )

    update match {
      case _: IngestEventUpdate =>
        Right(
          newIngest.copy(
            status = ingest.status match {
              case Ingest.Accepted => Ingest.Processing
              case _               => ingest.status
            }
          )
        )

      case update: IngestStatusUpdate =>
        matchIngestStatusUpdate(
          newIngest = newIngest,
          status = ingest.status,
          updateStatus = update.status
        )

      case update: IngestCallbackStatusUpdate =>
        matchCallbackUpdate(
          ingest = ingest,
          newIngest = newIngest,
          update = update
        )

      case update: IngestVersionUpdate =>
        matchIngestVersionUpdate(
          ingest = ingest,
          newIngest = newIngest,
          updateVersion = update.version
        )
    }
  }

  private def matchIngestStatusUpdate(
    newIngest: Ingest,
    status: Ingest.Status,
    updateStatus: Ingest.Status
  ): Either[IngestStatusGoingBackwardsError, Ingest] =
    if (!statusUpdateIsAllowed(status, updateStatus)) {
      Left(
        IngestStatusGoingBackwardsError(
          stored = status,
          update = updateStatus
        )
      )
    } else {
      Right(newIngest.copy(status = updateStatus))
    }

  private def matchIngestVersionUpdate(
    ingest: Ingest,
    newIngest: Ingest,
    updateVersion: BagVersion
  ): Either[MismatchedVersionUpdateError, Ingest] = ingest.version match {
    case Some(version) if version == updateVersion =>
      Right(newIngest)

    case Some(version) =>
      Left(MismatchedVersionUpdateError(version, updateVersion))

    case None =>
      Right(
        newIngest.copy(
          version = Some(updateVersion)
        )
      )
  }

  private def matchCallbackUpdate(
    ingest: Ingest,
    newIngest: Ingest,
    update: IngestCallbackStatusUpdate
  ): Either[IngestStoreError, Ingest] = ingest.callback match {

    case Some(callback)
        if !callbackStatusUpdateIsAllowed(
          callback.status,
          update.callbackStatus
        ) =>
      Left(
        IngestCallbackStatusGoingBackwardsError(
          stored = callback.status,
          update = update.callbackStatus
        )
      )

    case Some(callback) =>
      Right(
        newIngest
          .copy(callback = Some(callback.copy(status = update.callbackStatus)))
      )

    case None => Left(NoCallbackOnIngestError())
  }

  // The ingest statuses form a sort-of waterfall.  If you imagine them
  // in this order:
  //
  //    accepted
  //      +-> processing
  //            +-> failed
  //                  +-> succeeded
  //
  // You can move down, but you can't move back up again.
  //
  // e.g. once an ingest has moved to "processing" (an app has started doing
  // some work on it), then it can't move back to "accepted".
  //
  private def statusUpdateIsAllowed(
    initial: Ingest.Status,
    update: Ingest.Status
  ): Boolean =
    initial match {
      case status if status == update => true

      case Ingest.Accepted   => true
      case Ingest.Processing => update != Ingest.Accepted
      case Ingest.Failed     => update == Ingest.Succeeded
      case Ingest.Succeeded  => false

    }

  private def callbackStatusUpdateIsAllowed(
    initial: Callback.CallbackStatus,
    update: Callback.CallbackStatus
  ): Boolean =
    initial match {
      case status if status == update => true

      case Callback.Pending   => true
      case Callback.Succeeded => false
      case Callback.Failed    => false
    }
}

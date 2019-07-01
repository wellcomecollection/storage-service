package uk.ac.wellcome.platform.archive.common.ingests.tracker

import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.store.VersionedStore

import scala.util.{Failure, Success, Try}

private class IngestStatusGoingBackwardsException(val existing: Ingest.Status,
                                                  val update: Ingest.Status)
    extends RuntimeException(
      s"Received status update $update, but ingest already has status $existing")

private class CallbackStatusGoingBackwardsException(
  val existing: Callback.CallbackStatus,
  val update: Callback.CallbackStatus)
    extends RuntimeException(
      s"Received callback status update $update, but ingest already has status $existing")

private class NoCallbackException
    extends RuntimeException(
      "Received callback status update, but ingest doesn't have a callback")

trait IngestTracker {
  val underlying: VersionedStore[IngestID, Int, Ingest]

  type Result =
    Either[IngestTrackerError, Identified[Version[IngestID, Int], Ingest]]

  def init(ingest: Ingest): Result =
    underlying.init(ingest.id)(ingest) match {
      case Right(value) => Right(value)
      case Left(err: VersionAlreadyExistsError) =>
        Left(IngestAlreadyExistsError(err))
      case Left(err) => Left(IngestTrackerStoreError(err))
    }

  def get(id: IngestID): Result =
    underlying.getLatest(id) match {
      case Right(value)             => Right(value)
      case Left(err: NotFoundError) => Left(IngestDoesNotExistError(err))
      case Left(err)                => Left(IngestTrackerStoreError(err))
    }

  def update(update: IngestUpdate): Result = {
    val updateCallback =
      update match {
        case _: IngestEventUpdate =>
          (ingest: Ingest) =>
            ingest.copy(
              events = ingest.events ++ update.events
            )

        case statusUpdate: IngestStatusUpdate =>
          (ingest: Ingest) =>
            {
              if (!statusUpdateIsAllowed(ingest.status, statusUpdate.status)) {
                throw new IngestStatusGoingBackwardsException(
                  ingest.status,
                  statusUpdate.status)
              }

              ingest.copy(
                status = statusUpdate.status,
                events = ingest.events ++ update.events
              )
            }

        case callbackStatusUpdate: IngestCallbackStatusUpdate =>
          (ingest: Ingest) =>
            {
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
                      callback.copy(
                        status = callbackStatusUpdate.callbackStatus)),
                    events = ingest.events ++ update.events
                  )

                case None => throw new NoCallbackException()
              }
            }
      }

    Try { underlying.update(update.id)(updateCallback) } match {
      case Success(Right(result)) => Right(result)

      case Success(Left(err: UpdateNoSourceError)) =>
        Left(UpdateNonExistentIngestError(err))

      case Failure(err: IngestStatusGoingBackwardsException) =>
        Left(IngestStatusGoingBackwards(err.existing, err.update))

      case Failure(err: CallbackStatusGoingBackwardsException) =>
        Left(IngestCallbackStatusGoingBackwards(err.existing, err.update))

      case Failure(_: NoCallbackException) =>
        Left(NoCallbackOnIngest())

      case Success(Left(err)) => Left(IngestTrackerStoreError(err))

      case Failure(err) => Left(IngestTrackerUpdateError(err))
    }
  }

  /** Given a bag ID, find the ingests that it was involved in.
    *
    * This is intended to meet a particular use case for DLCS during migration and not as part of the
    * public/documented API.  Consider either removing this functionality or enhancing it to be fully
    * featured if a use case arises after migration.
    *
    */
  def listByBagId(bagId: BagId): Either[IngestTrackerError, Seq[Ingest]]

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

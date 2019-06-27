package uk.ac.wellcome.platform.archive.common.ingests.tracker

import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.store.VersionedStore

import scala.util.{Failure, Success, Try}

class IngestStatusGoingBackwardsException(val existing: Ingest.Status, val update: Ingest.Status)
  extends RuntimeException(s"Received status update $update, but ingest already has status $existing")

trait BetterIngestTracker {
  val underlying: VersionedStore[IngestID, Int, Ingest]

  type Result = Either[IngestTrackerError, Identified[Version[IngestID, Int], Ingest]]

  def init(ingest: Ingest): Result =
    underlying.init(ingest.id)(ingest) match {
      case Right(value)                         => Right(value)
      case Left(err: VersionAlreadyExistsError) => Left(IngestAlreadyExistsError(err))
      case Left(err)                            => Left(IngestTrackerStoreError(err))
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
          (ingest: Ingest) => {
            if (!statusUpdateIsAllowed(ingest.status, statusUpdate.status)) {
              throw new IngestStatusGoingBackwardsException(ingest.status, statusUpdate.status)
            }

            ingest.copy(
              bag = statusUpdate.affectedBag,
              status = statusUpdate.status,
              events = ingest.events ++ update.events
            )
          }


        case callbackStatusUpdate: IngestCallbackStatusUpdate =>
          (ingest: Ingest) =>
            // TODO: What if there's no status?
            ingest.copy(
              callback = ingest.callback.map { cb =>
                cb.copy(status = callbackStatusUpdate.callbackStatus)
              }
          )
      }

    Try { underlying.update(update.id)(updateCallback) } match {
      case Success(Right(result))                  => Right(result)

      case Success(Left(err: UpdateNoSourceError)) => Left(UpdateNonExistentIngestError(err))

      case Failure(err: IngestStatusGoingBackwardsException) =>
        Left(IngestStatusGoingBackwards(err.existing, err.update))

      case Failure(err)       => throw err
      case Success(Left(err)) => throw err.e


    }
  }

  private def statusUpdateIsAllowed(initial: Ingest.Status, update: Ingest.Status): Boolean =
    initial match {
      case Ingest.Accepted   => true
      case Ingest.Processing => update != Ingest.Accepted
      case Ingest.Completed  => false
      case Ingest.Failed     => false
    }
}

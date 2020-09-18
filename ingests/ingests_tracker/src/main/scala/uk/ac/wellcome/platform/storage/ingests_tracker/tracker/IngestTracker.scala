package uk.ac.wellcome.platform.storage.ingests_tracker.tracker

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.store.VersionedStore

trait IngestTracker extends Logging {
  val underlying: VersionedStore[IngestID, Int, Ingest]

  type Result =
    Either[IngestStoreError, Identified[Version[IngestID, Int], Ingest]]

  def init(ingest: Ingest): Result =
    underlying.init(ingest.id)(ingest) match {
      case Right(value) =>
        Right(value)
      case Left(err: VersionAlreadyExistsError) =>
        Left(IngestAlreadyExistsError(err))
      case Left(err: StorageError) =>
        Left(IngestStoreUnexpectedError(err))
    }

  def get(id: IngestID): Result =
    underlying.getLatest(id) match {
      case Right(value) =>
        Right(value)
      case Left(err: NotFoundError) =>
        Left(IngestDoesNotExistError(err))
      case Left(err: StorageError) =>
        Left(IngestStoreUnexpectedError(err))
    }

  def update(update: IngestUpdate): Result = {
    val updateCallback = (ingest: Ingest) =>
      IngestStates.applyUpdate(ingest, update).left.map(UpdateNotApplied)

    underlying.update(update.id)(updateCallback).left.map {
      updateError: UpdateError =>
        updateError.e match {
          case e: IngestStoreError => e
          case _ =>
            updateError match {
              case err: UpdateNoSourceError =>
                UpdateNonExistentIngestError(err)
              case err =>
                IngestStoreUnexpectedError(err)
            }
        }
    }
  }
}

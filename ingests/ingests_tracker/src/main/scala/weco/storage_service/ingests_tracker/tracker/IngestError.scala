package weco.storage_service.ingests_tracker.tracker

import weco.storage_service.bagit.models.BagVersion
import weco.storage_service.ingests.models.{Callback, Ingest}
import weco.storage.{
  NotFoundError,
  StorageError,
  UpdateNoSourceError,
  VersionAlreadyExistsError
}

sealed trait IngestStoreError extends Throwable
sealed trait StateConflictError extends IngestStoreError

case class IngestAlreadyExistsError(storageError: VersionAlreadyExistsError)
    extends StateConflictError

case class IngestDoesNotExistError(storageError: NotFoundError)
    extends StateConflictError

case class UpdateNonExistentIngestError(storageError: UpdateNoSourceError)
    extends StateConflictError

case class IngestStatusGoingBackwardsError(
  stored: Ingest.Status,
  update: Ingest.Status
) extends StateConflictError

case class IngestCallbackStatusGoingBackwardsError(
  stored: Callback.CallbackStatus,
  update: Callback.CallbackStatus
) extends StateConflictError

case class MismatchedVersionUpdateError(
  stored: BagVersion,
  update: BagVersion
) extends StateConflictError

case class NoCallbackOnIngestError() extends IngestStoreError

case class IngestStoreUnexpectedError(storageError: StorageError)
    extends IngestStoreError {
  override def toString: String = {
    s"IngestStoreUnexpectedError: $storageError"
  }
}

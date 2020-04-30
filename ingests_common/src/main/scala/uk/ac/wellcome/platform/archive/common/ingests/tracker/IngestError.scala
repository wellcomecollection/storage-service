package uk.ac.wellcome.platform.archive.common.ingests.tracker
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
import uk.ac.wellcome.platform.archive.common.ingests.models.{Callback, Ingest}
import uk.ac.wellcome.storage.{
  NotFoundError,
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

case class IngestStoreUnexpectedError(e: Throwable) extends IngestStoreError {

  override def toString: String = {
    s"IngestStoreUnexpectedError: ${e.toString}"
  }
}

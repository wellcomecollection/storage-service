package uk.ac.wellcome.platform.archive.common.ingests.tracker
import uk.ac.wellcome.platform.archive.common.ingests.models.{Callback, Ingest}
import uk.ac.wellcome.storage.{
  NotFoundError,
  StorageError,
  UpdateNoSourceError,
  VersionAlreadyExistsError
}

sealed trait IngestTrackerError

case class IngestTrackerStoreError(err: StorageError) extends IngestTrackerError

case class IngestAlreadyExistsError(err: VersionAlreadyExistsError)
    extends IngestTrackerError

case class IngestDoesNotExistError(err: NotFoundError)
    extends IngestTrackerError

case class UpdateNonExistentIngestError(err: UpdateNoSourceError)
    extends IngestTrackerError

case class IngestStatusGoingBackwards(
  stored: Ingest.Status,
  update: Ingest.Status
) extends IngestTrackerError

case class IngestCallbackStatusGoingBackwards(
  stored: Callback.CallbackStatus,
  update: Callback.CallbackStatus
) extends IngestTrackerError

case class NoCallbackOnIngest() extends IngestTrackerError

case class IngestTrackerUpdateError(err: Throwable) extends IngestTrackerError

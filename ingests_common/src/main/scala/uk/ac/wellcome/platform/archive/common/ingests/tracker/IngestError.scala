package uk.ac.wellcome.platform.archive.common.ingests.tracker
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
import uk.ac.wellcome.platform.archive.common.ingests.models.{Callback, Ingest}
import uk.ac.wellcome.storage.{NotFoundError, UpdateNoSourceError, VersionAlreadyExistsError}


sealed trait IngestStoreError extends Throwable

case class IngestAlreadyExistsError(storageError: VersionAlreadyExistsError)
    extends IngestStoreError

case class IngestDoesNotExistError(storageError: NotFoundError) extends IngestStoreError

case class UpdateNonExistentIngestError(storageError: UpdateNoSourceError)
    extends IngestStoreError

case class IngestStatusGoingBackwardsError(
  stored: Ingest.Status,
  update: Ingest.Status
) extends IngestStoreError

case class IngestCallbackStatusGoingBackwardsError(
  stored: Callback.CallbackStatus,
  update: Callback.CallbackStatus
) extends IngestStoreError

case class MismatchedVersionUpdateError(
  stored: BagVersion,
  update: BagVersion
) extends IngestStoreError

case class NoCallbackOnIngestError()
  extends IngestStoreError

case class IngestStoreUnexpectedError(e: Throwable) extends IngestStoreError

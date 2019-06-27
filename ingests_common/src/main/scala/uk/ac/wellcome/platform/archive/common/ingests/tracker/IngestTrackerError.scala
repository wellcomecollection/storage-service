package uk.ac.wellcome.platform.archive.common.ingests.tracker
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.storage.{NotFoundError, StorageError, UpdateNoSourceError, VersionAlreadyExistsError}

sealed trait IngestTrackerError

case class IngestTrackerStoreError(err: StorageError) extends IngestTrackerError

case class IngestAlreadyExistsError(err: VersionAlreadyExistsError) extends IngestTrackerError

case class IngestDoesNotExistError(err: NotFoundError) extends IngestTrackerError

case class UpdateNonExistentIngestError(err: UpdateNoSourceError) extends IngestTrackerError

case class IngestStatusGoingBackwards(existing: Ingest.Status, update: Ingest.Status) extends IngestTrackerError

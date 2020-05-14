package uk.ac.wellcome.platform.storage.ingests_tracker.client

import uk.ac.wellcome.platform.archive.common.ingests.models.IngestUpdate

sealed trait IngestTrackerError {
  val ingestUpdate: IngestUpdate
}
case class IngestTrackerConflictError(ingestUpdate: IngestUpdate)
    extends IngestTrackerError
case class IngestTrackerUnknownError(ingestUpdate: IngestUpdate, err: Throwable)
    extends IngestTrackerError

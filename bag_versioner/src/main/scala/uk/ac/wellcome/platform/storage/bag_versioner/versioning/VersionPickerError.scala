package uk.ac.wellcome.platform.storage.bag_versioner.versioning

import uk.ac.wellcome.storage.locking.FailedLockingServiceOp

sealed trait VersionPickerError

case class FailedToGetLock(failedLock: FailedLockingServiceOp)
    extends VersionPickerError

case class UnableToAssignVersion(
  ingestVersionManagerError: IngestVersionManagerError
) extends VersionPickerError

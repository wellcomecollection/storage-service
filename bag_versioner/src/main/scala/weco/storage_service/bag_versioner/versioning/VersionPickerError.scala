package weco.storage_service.bag_versioner.versioning

import weco.storage.locking.FailedLockingServiceOp

sealed trait VersionPickerError

case class FailedToGetLock(failedLock: FailedLockingServiceOp)
    extends VersionPickerError

case class UnableToAssignVersion(
  ingestVersionManagerError: IngestVersionManagerError
) extends VersionPickerError

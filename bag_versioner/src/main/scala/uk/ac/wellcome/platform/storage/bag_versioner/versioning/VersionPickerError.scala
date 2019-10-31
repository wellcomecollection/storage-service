package uk.ac.wellcome.platform.storage.bag_versioner.versioning

sealed trait VersionPickerError

case class InternalVersionPickerError(e: Throwable) extends VersionPickerError

case class UnableToAssignVersion(e: IngestVersionManagerError)
    extends VersionPickerError

case class IngestTypeCreateForExistingBag() extends VersionPickerError
case class IngestTypeUpdateForNewBag() extends VersionPickerError

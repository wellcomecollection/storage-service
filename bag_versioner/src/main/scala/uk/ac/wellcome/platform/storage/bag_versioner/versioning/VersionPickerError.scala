package uk.ac.wellcome.platform.storage.bag_versioner.versioning

import uk.ac.wellcome.platform.archive.common.versioning.IngestVersionManagerError

sealed trait VersionPickerError

case class InternalVersionPickerError(e: Throwable) extends VersionPickerError

case class UnableToAssignVersion(e: IngestVersionManagerError)
    extends VersionPickerError

case class IngestTypeCreateForExistingBag() extends VersionPickerError
case class IngestTypeUpdateForNewBag() extends VersionPickerError

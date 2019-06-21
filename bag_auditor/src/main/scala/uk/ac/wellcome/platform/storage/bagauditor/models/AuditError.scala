package uk.ac.wellcome.platform.storage.bagauditor.models

import uk.ac.wellcome.platform.archive.common.versioning.IngestVersionManagerError

sealed trait AuditError

case class CannotFindExternalIdentifier(e: Throwable) extends AuditError

sealed trait VersionPickerError extends AuditError

case class InternalVersionPickerError(e: Throwable) extends VersionPickerError

case class UnableToAssignVersion(e: IngestVersionManagerError) extends VersionPickerError

case class IngestTypeCreateForExistingBag() extends VersionPickerError
case class IngestTypeUpdateForNewBag() extends VersionPickerError

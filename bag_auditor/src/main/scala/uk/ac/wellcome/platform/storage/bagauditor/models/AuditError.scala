package uk.ac.wellcome.platform.storage.bagauditor.models

sealed trait AuditError

case class CannotFindExternalIdentifier(e: Throwable) extends AuditError

sealed trait VersionPickerError extends AuditError

case class InternalVersionPickerError(e: Throwable) extends VersionPickerError

case class IngestTypeCreateForExistingBag() extends VersionPickerError
case class IngestTypeUpdateForNewBag() extends VersionPickerError

package uk.ac.wellcome.platform.archive.common.versioning

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier

sealed trait IngestVersionManagerError

case class InternalVersionManagerError(e: Throwable) extends IngestVersionManagerError

case class ExternalIdentifiersMismatch(stored: ExternalIdentifier, request: ExternalIdentifier) extends IngestVersionManagerError

case class NewerIngestAlreadyExists(stored: Instant, request: Instant) extends IngestVersionManagerError
package uk.ac.wellcome.platform.archive.common.versioning

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace

sealed trait IngestVersionManagerError

case class IngestVersionManagerDaoError(e: Throwable)
    extends IngestVersionManagerError

case class ExternalIdentifiersMismatch(stored: ExternalIdentifier,
                                       request: ExternalIdentifier)
    extends IngestVersionManagerError

case class StorageSpaceMismatch(stored: StorageSpace, request: StorageSpace)
    extends IngestVersionManagerError

case class NewerIngestAlreadyExists(stored: Instant, request: Instant)
    extends IngestVersionManagerError

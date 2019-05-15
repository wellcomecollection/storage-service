package uk.ac.wellcome.platform.archive.common.versioning

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier

case class VersionRecord(
  externalIdentifier: ExternalIdentifier,
  ingestId: IngestID,
  ingestDate: Instant,
  version: Int
)

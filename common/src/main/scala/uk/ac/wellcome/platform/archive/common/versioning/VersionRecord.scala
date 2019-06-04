package uk.ac.wellcome.platform.archive.common.versioning

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID

case class VersionRecord(
  externalIdentifier: ExternalIdentifier,
  ingestId: IngestID,
  ingestDate: Instant,
  version: Int
)

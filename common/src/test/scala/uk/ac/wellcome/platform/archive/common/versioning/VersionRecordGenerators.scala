package uk.ac.wellcome.platform.archive.common.versioning

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.generators.ExternalIdentifierGenerators

trait VersionRecordGenerators extends ExternalIdentifierGenerators {
  def createVersionRecordWith(
    externalIdentifier: ExternalIdentifier = createExternalIdentifier,
    ingestId: IngestID = createIngestID,
    version: Int = 1
  ): VersionRecord =
    VersionRecord(
      externalIdentifier = externalIdentifier,
      ingestId = ingestId,
      ingestDate = Instant.now,
      version = version
    )

  def createVersionRecord: VersionRecord = createVersionRecordWith()
}

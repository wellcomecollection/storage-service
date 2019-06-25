package uk.ac.wellcome.platform.archive.common.versioning

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.generators.{ExternalIdentifierGenerators, StorageSpaceGenerators}
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace

trait VersionRecordGenerators extends ExternalIdentifierGenerators with StorageSpaceGenerators {
  def createVersionRecordWith(
    externalIdentifier: ExternalIdentifier = createExternalIdentifier,
    ingestId: IngestID = createIngestID,
    storageSpace: StorageSpace = createStorageSpace,
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

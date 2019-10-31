package uk.ac.wellcome.platform.storage.bag_versioner.versioning

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagVersion,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.generators.{
  ExternalIdentifierGenerators,
  StorageSpaceGenerators
}
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace

import scala.util.Random

trait VersionRecordGenerators
    extends ExternalIdentifierGenerators
    with StorageSpaceGenerators {
  def createVersionRecordWith(
    externalIdentifier: ExternalIdentifier = createExternalIdentifier,
    ingestId: IngestID = createIngestID,
    storageSpace: StorageSpace = createStorageSpace,
    version: Int = Random.nextInt
  ): VersionRecord =
    VersionRecord(
      externalIdentifier = externalIdentifier,
      ingestId = ingestId,
      ingestDate = Instant.now,
      storageSpace = storageSpace,
      version = BagVersion(version)
    )

  def createVersionRecord: VersionRecord = createVersionRecordWith()
}

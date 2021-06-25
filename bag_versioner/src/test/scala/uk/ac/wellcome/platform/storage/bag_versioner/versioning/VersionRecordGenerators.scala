package uk.ac.wellcome.platform.storage.bag_versioner.versioning

import java.time.Instant

import weco.storage_service.bagit.models.{
  BagVersion,
  ExternalIdentifier
}
import weco.storage.generators.{
  ExternalIdentifierGenerators,
  StorageSpaceGenerators
}
import weco.storage_service.ingests.models.IngestID
import weco.storage_service.storage.models.StorageSpace

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

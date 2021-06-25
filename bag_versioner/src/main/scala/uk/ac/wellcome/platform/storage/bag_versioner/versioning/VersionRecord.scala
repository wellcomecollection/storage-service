package uk.ac.wellcome.platform.storage.bag_versioner.versioning

import java.time.Instant

import weco.storage_service.bagit.models.{
  BagVersion,
  ExternalIdentifier
}
import weco.storage_service.ingests.models.IngestID
import weco.storage_service.storage.models.StorageSpace

case class VersionRecord(
  externalIdentifier: ExternalIdentifier,
  ingestId: IngestID,
  ingestDate: Instant,
  storageSpace: StorageSpace,
  version: BagVersion
)

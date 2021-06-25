package weco.storage_service

import java.time.Instant

import weco.storage_service.bagit.models.ExternalIdentifier
import weco.storage_service.ingests.models.{
  Ingest,
  IngestID,
  IngestType
}
import weco.storage_service.storage.models.StorageSpace

case class PipelineContext(
  ingestId: IngestID,
  ingestType: IngestType,
  storageSpace: StorageSpace,
  ingestDate: Instant,
  externalIdentifier: ExternalIdentifier
)

case object PipelineContext {
  def apply(ingest: Ingest): PipelineContext =
    PipelineContext(
      ingestId = ingest.id,
      ingestType = ingest.ingestType,
      storageSpace = StorageSpace(ingest.space.underlying),
      ingestDate = ingest.createdDate,
      externalIdentifier = ingest.externalIdentifier
    )
}

package uk.ac.wellcome.platform.archive.common

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  IngestID,
  IngestType
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace

case class PipelineContext(
  ingestId: IngestID,
  ingestType: IngestType,
  storageSpace: StorageSpace,
  ingestDate: Instant
)

case object PipelineContext {
  def apply(ingest: Ingest): PipelineContext =
    PipelineContext(
      ingestId = ingest.id,
      ingestType = ingest.ingestType,
      storageSpace = StorageSpace(ingest.space.underlying),
      ingestDate = ingest.createdDate
    )
}

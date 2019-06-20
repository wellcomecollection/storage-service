package uk.ac.wellcome.platform.archive.common

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID, IngestType}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation

sealed trait PipelinePayload {
  def ingestId: IngestID
}

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

sealed trait BetterPipelinePayload extends PipelinePayload {
  val context: PipelineContext

  def ingestId: IngestID = context.ingestId
  def ingestType: IngestType = context.ingestType
  def storageSpace: StorageSpace = context.storageSpace
  def ingestDate: Instant = context.ingestDate
}

case class SourceLocationPayload(
  context: PipelineContext,
  sourceLocation: ObjectLocation
) extends BetterPipelinePayload

case object SourceLocationPayload {
  def apply(ingest: Ingest): SourceLocationPayload =
    SourceLocationPayload(
      context = PipelineContext(ingest),
      sourceLocation = ingest.sourceLocation.location
    )
}

case class UnpackedBagPayload(
  ingestId: IngestID,
  ingestDate: Instant,
  storageSpace: StorageSpace,
  unpackedBagLocation: ObjectLocation
) extends PipelinePayload

case object UnpackedBagPayload {
  def apply(sourceLocationPayload: SourceLocationPayload,
            unpackedBagLocation: ObjectLocation): UnpackedBagPayload =
    UnpackedBagPayload(
      ingestId = sourceLocationPayload.ingestId,
      ingestDate = sourceLocationPayload.ingestDate,
      storageSpace = sourceLocationPayload.storageSpace,
      unpackedBagLocation = unpackedBagLocation
    )
}

sealed trait BagRootPayload extends PipelinePayload {
  val bagRootLocation: ObjectLocation
}

case class BagInformationPayload(
  ingestId: IngestID,
  storageSpace: StorageSpace,
  bagRootLocation: ObjectLocation
) extends BagRootPayload

case class EnrichedBagInformationPayload(
  ingestId: IngestID,
  storageSpace: StorageSpace,
  bagRootLocation: ObjectLocation,
  externalIdentifier: ExternalIdentifier,
  version: Int
) extends BagRootPayload

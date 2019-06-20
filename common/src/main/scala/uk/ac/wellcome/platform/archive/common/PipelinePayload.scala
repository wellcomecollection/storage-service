package uk.ac.wellcome.platform.archive.common

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID, IngestType}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation

sealed trait PipelinePayload {
  def ingestId: IngestID
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

case class UnpackedBagLocationPayload(
  context: PipelineContext,
  unpackedBagLocation: ObjectLocation
) extends BetterPipelinePayload

case object UnpackedBagLocationPayload {
  def apply(payload: BetterPipelinePayload,
            unpackedBagLocation: ObjectLocation): UnpackedBagLocationPayload =
    UnpackedBagLocationPayload(
      context = payload.context,
      unpackedBagLocation = unpackedBagLocation
    )
}

sealed trait BagRootPayload extends BetterPipelinePayload {
  val bagRootLocation: ObjectLocation
}

case class BagRootLocationPayload(
  context: PipelineContext,
  bagRootLocation: ObjectLocation
) extends BagRootPayload

case class EnrichedBagInformationPayload(
  context: PipelineContext,
  bagRootLocation: ObjectLocation,
  externalIdentifier: ExternalIdentifier,
  version: Int
) extends BagRootPayload

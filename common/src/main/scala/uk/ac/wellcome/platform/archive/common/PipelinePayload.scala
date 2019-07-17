package uk.ac.wellcome.platform.archive.common

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  IngestID,
  IngestType
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

/** This is the base trait for messages passed between services.
  *
  * As of 17 July 2019, this is the way payloads are passed between apps:
  *
  *    ┌─────────────────┐
  *    │   ingests API   │───────────▶ SourceLocationPayload
  *    └─────────────────┘
  *
  *    ┌─────────────────┐
  *    │  bag unpacker   │
  *    └─────────────────┘
  *             │
  *             └───── +unpack ──────▶ UnpackedBagLocationPayload
  *                    location
  *
  *    ┌─────────────────┐
  *    │ bag root finder │
  *    └─────────────────┘
  *             │
  *             └────── +root ───────▶ BagRootLocationPayload
  *                    location
  *
  *    ┌─────────────────┐
  *    │  bag verifier   │───────────▶ BagRootLocationPayload
  *    └─────────────────┘
  *
  *    ┌─────────────────┐
  *    │  bag versioner  │
  *    └─────────────────┘
  *             │
  *             └───── +version──────▶ VersionedBagRootLocationPayload
  *
  *    ┌─────────────────┐
  *    │ bag replicator  │
  *    └─────────────────┘
  *             │
  *             └───── +replica ─────▶ BagReplicaLocationPayload
  *                    location
  *
  *    ┌─────────────────┐
  *    │  bag verifier   │───────────▶ BagReplicaLocationPayload
  *    └─────────────────┘
  */
sealed trait PipelinePayload {
  val context: PipelineContext

  def ingestId: IngestID = context.ingestId
  def ingestType: IngestType = context.ingestType
  def storageSpace: StorageSpace = context.storageSpace
  def ingestDate: Instant = context.ingestDate
  def externalIdentifier: ExternalIdentifier = context.externalIdentifier
}

case class SourceLocationPayload(
  context: PipelineContext,
  sourceLocation: ObjectLocation
) extends PipelinePayload

case object SourceLocationPayload {
  def apply(ingest: Ingest): SourceLocationPayload =
    SourceLocationPayload(
      context = PipelineContext(ingest),
      sourceLocation = ingest.sourceLocation.location
    )
}

case class UnpackedBagLocationPayload(
  context: PipelineContext,
  unpackedBagLocation: ObjectLocationPrefix
) extends PipelinePayload

sealed trait BagRootPayload extends PipelinePayload {
  val bagRootLocation: ObjectLocation
}

case class BagRootLocationPayload(
  context: PipelineContext,
  bagRootLocation: ObjectLocation
) extends BagRootPayload

case class VersionedBagRootLocationPayload(
  context: PipelineContext,
  bagRootLocation: ObjectLocation,
  version: Int
) extends BagRootPayload

case class BagReplicaLocationPayload(
  context: PipelineContext,
  replicaLocation: BagReplicaLocation,
  version: Int
) extends BagRootPayload {
  override val bagRootLocation: ObjectLocation = replicaLocation.asLocation
}

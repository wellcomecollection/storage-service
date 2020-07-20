package uk.ac.wellcome.platform.archive.common

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagVersion,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  IngestID,
  IngestType,
  SourceLocation
}
import uk.ac.wellcome.platform.archive.common.storage.models.{
  KnownReplicas,
  ReplicaResult,
  StorageSpace
}
import uk.ac.wellcome.storage.{ObjectLocationPrefix, S3ObjectLocationPrefix}

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
  sourceLocation: SourceLocation
) extends PipelinePayload

case object SourceLocationPayload {
  def apply(ingest: Ingest): SourceLocationPayload =
    SourceLocationPayload(
      context = PipelineContext(ingest),
      sourceLocation = ingest.sourceLocation
    )
}

case class UnpackedBagLocationPayload(
  context: PipelineContext,
  unpackedBagLocation: S3ObjectLocationPrefix
) extends PipelinePayload

case class KnownReplicasPayload(
  context: PipelineContext,
  version: BagVersion,
  knownReplicas: KnownReplicas
) extends PipelinePayload

case class BagRootLocationPayload(
  context: PipelineContext,
  bagRoot: ObjectLocationPrefix
) extends PipelinePayload

sealed trait VerifiablePayload extends PipelinePayload {
  val bagRoot: ObjectLocationPrefix
}

case class VersionedBagRootPayload(
  context: PipelineContext,
  bagRoot: ObjectLocationPrefix,
  version: BagVersion
) extends VerifiablePayload

case class ReplicaResultPayload(
  context: PipelineContext,
  replicaResult: ReplicaResult,
  version: BagVersion
) extends VerifiablePayload {
  val bagRoot: ObjectLocationPrefix =
    replicaResult.storageLocation.prefix
}

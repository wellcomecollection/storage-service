package weco.storage_service.generators

import java.time.Instant
import weco.storage_service.bagit.models.{BagVersion, ExternalIdentifier}
import weco.storage_service.ingests.models._
import weco.storage_service.storage.models._
import weco.storage.providers.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import weco.storage_service._

trait PayloadGenerators
    extends ExternalIdentifierGenerators
    with StorageSpaceGenerators
    with ReplicaLocationGenerators {

  def randomIngestType: IngestType =
    chooseFrom(CreateIngestType, UpdateIngestType)

  def createPipelineContextWith(
    ingestId: IngestID = createIngestID,
    ingestType: IngestType = randomIngestType,
    ingestDate: Instant = randomInstant,
    storageSpace: StorageSpace = createStorageSpace,
    externalIdentifier: ExternalIdentifier = createExternalIdentifier
  ): PipelineContext =
    PipelineContext(
      ingestId = ingestId,
      ingestType = ingestType,
      storageSpace = storageSpace,
      ingestDate = ingestDate,
      externalIdentifier = externalIdentifier
    )

  def createPipelineContext: PipelineContext =
    createPipelineContextWith()

  def createSourceLocationPayloadWith(
    sourceLocation: S3ObjectLocation = createS3ObjectLocation,
    storageSpace: StorageSpace = createStorageSpace
  ): SourceLocationPayload =
    SourceLocationPayload(
      context = createPipelineContextWith(
        storageSpace = storageSpace
      ),
      sourceLocation = S3SourceLocation(sourceLocation)
    )

  def createSourceLocationPayload: SourceLocationPayload =
    createSourceLocationPayloadWith()

  def createUnpackedBagLocationPayloadWith(
    unpackedBagLocation: S3ObjectLocationPrefix = createS3ObjectLocationPrefix,
    storageSpace: StorageSpace = createStorageSpace
  ): UnpackedBagLocationPayload =
    UnpackedBagLocationPayload(
      context = createPipelineContextWith(
        storageSpace = storageSpace
      ),
      unpackedBagLocation = unpackedBagLocation
    )

  def createKnownReplicas = KnownReplicas(
    location = createPrimaryLocation,
    replicas = (1 to randomInt(from = 0, to = 5)).map { _ =>
      createSecondaryLocation
    }.toList
  )

  def createKnownReplicasPayload: KnownReplicasPayload =
    createKnownReplicasPayloadWith()

  def createKnownReplicasPayloadWith(
    context: PipelineContext = createPipelineContext,
    version: BagVersion = createBagVersion,
    knownReplicas: KnownReplicas = createKnownReplicas
  ) = KnownReplicasPayload(
    context = context,
    version = version,
    knownReplicas = knownReplicas
  )

  def createVersionedBagRootPayloadWith(
    context: PipelineContext = createPipelineContext,
    bagRoot: S3ObjectLocationPrefix = createS3ObjectLocationPrefix,
    version: BagVersion = createBagVersion
  ): VersionedBagRootPayload =
    VersionedBagRootPayload(
      context = context,
      bagRoot = bagRoot,
      version = version
    )

  def createVersionedBagRootPayload: VersionedBagRootPayload =
    createVersionedBagRootPayloadWith()

  def createBagRootLocationPayloadWith(
    context: PipelineContext = createPipelineContext,
    bagRoot: S3ObjectLocationPrefix = createS3ObjectLocationPrefix
  ): BagRootLocationPayload =
    BagRootLocationPayload(
      context = context,
      bagRoot = bagRoot
    )

  def createReplicaCompletePayloadWith(
    dstLocation: ReplicaLocation = createPrimaryLocation
  ): ReplicaCompletePayload =
    ReplicaCompletePayload(
      context = createPipelineContext,
      srcPrefix = createS3ObjectLocationPrefix,
      dstLocation = dstLocation,
      version = createBagVersion
    )

  def createReplicaCompletePayload: ReplicaCompletePayload =
    createReplicaCompletePayloadWith()
}

package uk.ac.wellcome.platform.archive.common.generators

import java.time.Instant

import uk.ac.wellcome.platform.archive.common._
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagVersion,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  CreateIngestType,
  InfrequentAccessStorageProvider,
  IngestID,
  IngestType,
  UpdateIngestType
}
import uk.ac.wellcome.platform.archive.common.storage.models.{
  KnownReplicas,
  PrimaryStorageLocation,
  SecondaryStorageLocation,
  StorageSpace
}
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators

import scala.util.Random

trait PayloadGenerators
    extends ExternalIdentifierGenerators
    with StorageSpaceGenerators
    with ObjectLocationGenerators {

  def randomIngestType: IngestType =
    Seq(CreateIngestType, UpdateIngestType)(Random.nextInt(1))

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
    sourceLocation: ObjectLocation = createObjectLocation,
    storageSpace: StorageSpace = createStorageSpace
  ): SourceLocationPayload =
    SourceLocationPayload(
      context = createPipelineContextWith(
        storageSpace = storageSpace
      ),
      sourceLocation = sourceLocation
    )

  def createSourceLocationPayload: SourceLocationPayload =
    createSourceLocationPayloadWith()

  def createUnpackedBagLocationPayloadWith(
    unpackedBagLocation: ObjectLocationPrefix = createObjectLocationPrefix,
    storageSpace: StorageSpace = createStorageSpace
  ): UnpackedBagLocationPayload =
    UnpackedBagLocationPayload(
      context = createPipelineContextWith(
        storageSpace = storageSpace
      ),
      unpackedBagLocation = unpackedBagLocation
    )

  def createKnownReplicas = KnownReplicas(
    location = PrimaryStorageLocation(
      provider = InfrequentAccessStorageProvider,
      prefix = createObjectLocationPrefix
    ),
    replicas = (1 to randomInt(from = 0, to = 5))
      .map(
        _ =>
          SecondaryStorageLocation(
            provider = InfrequentAccessStorageProvider,
            prefix = createObjectLocationPrefix
          )
      )
      .toList
  )

  def createKnownReplicasPayload =
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

  def createEnrichedBagInformationPayloadWith(
    context: PipelineContext = createPipelineContext,
    bagRoot: ObjectLocationPrefix = createObjectLocationPrefix,
    version: BagVersion = createBagVersion
  ): EnrichedBagInformationPayload =
    EnrichedBagInformationPayload(
      context = context,
      bagRoot = bagRoot,
      version = version
    )

  def createEnrichedBagInformationPayload: EnrichedBagInformationPayload =
    createEnrichedBagInformationPayloadWith()

  def createBagRootLocationPayloadWith(
    context: PipelineContext = createPipelineContext,
    bagRoot: ObjectLocationPrefix = createObjectLocationPrefix
  ): BagRootLocationPayload =
    BagRootLocationPayload(
      context = context,
      bagRoot = bagRoot
    )

  def createBagRootLocationPayload: BagRootLocationPayload =
    createBagRootLocationPayloadWith()
}

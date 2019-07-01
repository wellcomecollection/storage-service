package uk.ac.wellcome.platform.archive.common.generators

import java.time.Instant

import uk.ac.wellcome.platform.archive.common._
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  CreateIngestType,
  IngestID,
  IngestType,
  UpdateIngestType
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation
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
    ingestDate: Instant = Instant.now(),
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

  def createSourceLocation: SourceLocationPayload =
    createSourceLocationPayloadWith()

  def createUnpackedBagLocationPayloadWith(
    unpackedBagLocation: ObjectLocation = createObjectLocation,
    storageSpace: StorageSpace = createStorageSpace
  ): UnpackedBagLocationPayload =
    UnpackedBagLocationPayload(
      context = createPipelineContextWith(
        storageSpace = storageSpace
      ),
      unpackedBagLocation = unpackedBagLocation
    )

  def createEnrichedBagInformationPayloadWith(
    context: PipelineContext = createPipelineContext,
    bagRootLocation: ObjectLocation = createObjectLocation,
    version: Int = 1
  ): EnrichedBagInformationPayload =
    EnrichedBagInformationPayload(
      context = context,
      bagRootLocation = bagRootLocation,
      version = version
    )

  def createEnrichedBagInformationPayload: EnrichedBagInformationPayload =
    createEnrichedBagInformationPayloadWith()

  // TODO: Just pass the context directly.
  def createBagRootLocationPayloadWith(
    ingestId: IngestID = createIngestID,
    ingestDate: Instant = Instant.now(),
    bagRootLocation: ObjectLocation = createObjectLocation,
    externalIdentifier: ExternalIdentifier = createExternalIdentifier,
    storageSpace: StorageSpace = createStorageSpace): BagRootLocationPayload =
    BagRootLocationPayload(
      context = createPipelineContextWith(
        ingestId = ingestId,
        ingestDate = ingestDate,
        storageSpace = storageSpace,
        externalIdentifier = externalIdentifier
      ),
      bagRootLocation = bagRootLocation
    )

  def createBagRootLocationPayload: BagRootLocationPayload =
    createBagRootLocationPayloadWith()
}

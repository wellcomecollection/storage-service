package uk.ac.wellcome.platform.archive.common.generators

import java.time.Instant

import uk.ac.wellcome.platform.archive.common._
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  CreateIngestType,
  IngestID
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators

trait PayloadGenerators
    extends ExternalIdentifierGenerators
    with StorageSpaceGenerators
    with ObjectLocationGenerators {

  def createPipelineContextWith(
    ingestId: IngestID = createIngestID,
    ingestDate: Instant = Instant.now(),
    storageSpace: StorageSpace = createStorageSpace
  ): PipelineContext =
    PipelineContext(
      ingestId = ingestId,
      ingestType = CreateIngestType,
      storageSpace = storageSpace,
      ingestDate = ingestDate
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

  def createEnrichedBagInformationPayload(
    ingestId: IngestID = createIngestID,
    ingestDate: Instant = Instant.now,
    bagRootLocation: ObjectLocation = createObjectLocation,
    storageSpace: StorageSpace = createStorageSpace,
    externalIdentifier: ExternalIdentifier = createExternalIdentifier,
    version: Int = 1): EnrichedBagInformationPayload =
    EnrichedBagInformationPayload(
      context = createPipelineContextWith(
        ingestId = ingestId,
        ingestDate = ingestDate,
        storageSpace = storageSpace
      ),
      bagRootLocation = bagRootLocation,
      externalIdentifier = externalIdentifier,
      version = version
    )

  def createEnrichedBagInformationPayload: EnrichedBagInformationPayload =
    createEnrichedBagInformationPayload()

  def createBagRootLocationPayloadWith(
    bagRootLocation: ObjectLocation): BagRootLocationPayload =
    BagRootLocationPayload(
      context = createPipelineContext,
      bagRootLocation = bagRootLocation
    )
}

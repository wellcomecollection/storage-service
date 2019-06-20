package uk.ac.wellcome.platform.archive.common.generators

import java.time.Instant

import uk.ac.wellcome.platform.archive.common._
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models.{CreateIngestType, IngestID}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators

trait PayloadGenerators
    extends ExternalIdentifierGenerators
    with StorageSpaceGenerators
    with ObjectLocationGenerators {

  def createPipelineContextWith(
    storageSpace: StorageSpace = createStorageSpace
  ): PipelineContext =
    PipelineContext(
      ingestId = createIngestID,
      ingestType = CreateIngestType,
      storageSpace = storageSpace,
      ingestDate = Instant.now()
    )

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

  def createUnpackedBagPayloadWith(
    unpackedBagLocation: ObjectLocation = createObjectLocation,
    storageSpace: StorageSpace = createStorageSpace
  ): UnpackedBagPayload =
    UnpackedBagPayload(
      ingestId = createIngestID,
      ingestDate = Instant.now(),
      storageSpace = storageSpace,
      unpackedBagLocation = unpackedBagLocation
    )

  def createEnrichedBagInformationPayload(
    ingestId: IngestID = createIngestID,
    bagRootLocation: ObjectLocation = createObjectLocation,
    storageSpace: StorageSpace = createStorageSpace,
    externalIdentifier: ExternalIdentifier = createExternalIdentifier,
    version: Int = 1): EnrichedBagInformationPayload =
    EnrichedBagInformationPayload(
      ingestId = ingestId,
      storageSpace = storageSpace,
      bagRootLocation = bagRootLocation,
      externalIdentifier = externalIdentifier,
      version = version
    )

  def createEnrichedBagInformationPayload: EnrichedBagInformationPayload =
    createEnrichedBagInformationPayload()

  def createBagInformationPayloadWith(
    bagRootLocation: ObjectLocation): BagInformationPayload =
    BagInformationPayload(
      ingestId = createIngestID,
      storageSpace = createStorageSpace,
      bagRootLocation = bagRootLocation
    )
}

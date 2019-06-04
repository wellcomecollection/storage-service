package uk.ac.wellcome.platform.archive.common.generators

import java.time.Instant

import uk.ac.wellcome.platform.archive.common._
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators

trait PayloadGenerators
    extends ExternalIdentifierGenerators
    with StorageSpaceGenerators
    with ObjectLocationGenerators {
  def createIngestRequestPayloadWith(
    sourceLocation: ObjectLocation = createObjectLocation,
    storageSpace: StorageSpace = createStorageSpace
  ): IngestRequestPayload =
    IngestRequestPayload(
      ingestId = createIngestID,
      ingestDate = Instant.now(),
      storageSpace = storageSpace,
      sourceLocation = sourceLocation
    )

  def createIngestRequestPayload: IngestRequestPayload =
    createIngestRequestPayloadWith()

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

  def createBagInformationPayloadWith(
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

  def createBagInformationPayload: EnrichedBagInformationPayload =
    createBagInformationPayloadWith()
}

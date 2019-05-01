package uk.ac.wellcome.platform.archive.common.generators

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.{
  BagInformationPayload,
  IngestID,
  IngestRequestPayload,
  ObjectLocationPayload
}
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3

trait PayloadGenerators
    extends ExternalIdentifierGenerators
    with StorageSpaceGenerators
    with S3 {
  def createObjectLocationPayloadWith(
    objectLocation: ObjectLocation = createObjectLocation,
    storageSpace: StorageSpace = createStorageSpace): ObjectLocationPayload =
    ObjectLocationPayload(
      ingestId = createIngestID,
      storageSpace = storageSpace,
      objectLocation = objectLocation
    )

  def createObjectLocationPayload: ObjectLocationPayload =
    createObjectLocationPayloadWith()

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

  def createBagInformationPayloadWith(
    ingestId: IngestID = createIngestID,
    bagRootLocation: ObjectLocation = createObjectLocation,
    storageSpace: StorageSpace = createStorageSpace,
    externalIdentifier: ExternalIdentifier = createExternalIdentifier,
    version: Int = 1): BagInformationPayload =
    BagInformationPayload(
      ingestId = ingestId,
      storageSpace = storageSpace,
      bagRootLocation = bagRootLocation,
      externalIdentifier = externalIdentifier,
      version = version
    )

  def createBagInformationPayload: BagInformationPayload =
    createBagInformationPayloadWith()
}

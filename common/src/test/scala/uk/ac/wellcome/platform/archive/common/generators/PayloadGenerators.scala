package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.ObjectLocationPayload
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3

trait PayloadGenerators extends StorageSpaceGenerators with S3 {
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
}

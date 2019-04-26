package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.ObjectLocationPayload
import uk.ac.wellcome.storage.ObjectLocation

trait PayloadGenerators extends StorageSpaceGenerators {
  def createObjectLocationPayloadWith(objectLocation: ObjectLocation): ObjectLocationPayload =
    ObjectLocationPayload(
      ingestId = createIngestID,
      storageSpace = createStorageSpace,
      objectLocation = objectLocation
    )
}

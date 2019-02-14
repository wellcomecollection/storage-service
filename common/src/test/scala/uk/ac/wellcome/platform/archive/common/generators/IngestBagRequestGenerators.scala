package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.models.IngestBagRequest
import uk.ac.wellcome.storage.ObjectLocation

trait IngestBagRequestGenerators extends StorageSpaceGenerators {
  def createIngestBagRequest: IngestBagRequest = createIngestBagRequestWith()

  def createIngestBagRequestWith(
    ingestBagLocation: ObjectLocation =
      ObjectLocation("testNamespace", "testKey")) =
    IngestBagRequest(
      id = randomUUID,
      zippedBagLocation = ingestBagLocation,
      archiveCompleteCallbackUrl = None,
      storageSpace = createStorageSpace
    )
}

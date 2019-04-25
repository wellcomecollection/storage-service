package uk.ac.wellcome.platform.archive.bagunpacker.fixtures

import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.generators.{
  BagLocationGenerators,
  StorageSpaceGenerators
}
import uk.ac.wellcome.platform.archive.common.ingests.models.UnpackBagRequest
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3

trait UnpackBagRequestGenerators
    extends BagLocationGenerators
    with StorageSpaceGenerators
    with S3 {
  def createUnpackBagRequest: UnpackBagRequest =
    createUnpackBagRequestWith()

  def createUnpackBagRequestWith(
    ingestId: IngestID = createIngestID,
    sourceLocation: ObjectLocation = createObjectLocation,
    storageSpace: StorageSpace = createStorageSpace
  ): UnpackBagRequest =
    UnpackBagRequest(
      ingestId = ingestId,
      sourceLocation = sourceLocation,
      storageSpace = storageSpace
    )
}

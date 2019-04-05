package uk.ac.wellcome.platform.archive.bagunpacker.fixtures

import java.util.UUID

import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.generators.{
  BagLocationGenerators,
  StorageSpaceGenerators
}
import uk.ac.wellcome.platform.archive.common.ingests.models.UnpackBagRequest
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3

trait UnpackBagRequestGenerators
    extends RandomThings
    with BagLocationGenerators
    with StorageSpaceGenerators
    with S3 {
  def createUnpackBagRequest: UnpackBagRequest =
    createUnpackBagRequestWith()

  def createUnpackBagRequestWith(
    requestId: UUID = randomUUID,
    sourceLocation: ObjectLocation = createObjectLocation,
    storageSpace: StorageSpace = createStorageSpace
  ): UnpackBagRequest =
    UnpackBagRequest(
      requestId = requestId,
      sourceLocation = sourceLocation,
      storageSpace = storageSpace
    )
}

package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.models.StorageSpace
import uk.ac.wellcome.platform.archive.common.models.bagit.{BagLocation, BagPath, ExternalIdentifier}
import uk.ac.wellcome.storage.fixtures.S3.Bucket

trait BagLocationGenerators extends ExternalIdentifierGenerators with StorageSpaceGenerators {
  def createBagLocationWith(
    bucket: Bucket = Bucket(randomAlphanumeric()),
    storagePrefix: Option[String] = None,
    storageSpace: StorageSpace = createStorageSpace,
    bagIdentifier: ExternalIdentifier = createExternalIdentifier): BagLocation =
    BagLocation(
      storageNamespace = bucket.name,
      storagePrefix = storagePrefix,
      storageSpace = createStorageSpace,
      bagPath = BagPath(bagIdentifier.toString)
    )

  def createBagLocation: BagLocation = createBagLocationWith()
}

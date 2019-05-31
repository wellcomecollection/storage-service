package uk.ac.wellcome.platform.archive.common.generators

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagFile,
  BagInfo,
  BagManifest,
  BagPath
}
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  StandardStorageProvider,
  StorageLocation
}
import uk.ac.wellcome.platform.archive.common.storage.models.{
  StorageManifest,
  StorageSpace
}
import uk.ac.wellcome.platform.archive.common.verify.{
  Checksum,
  ChecksumValue,
  SHA256
}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators

trait StorageManifestGenerators
    extends BagInfoGenerators
    with StorageSpaceGenerators
    with ObjectLocationGenerators {

  val checksumAlgorithm = SHA256
  val checksumValue = ChecksumValue("a")

  val bagItemPath = BagPath("bag-info.txt")
  val manifestFiles = List(
    BagFile(
      Checksum(
      checksumAlgorithm,
        checksumValue
      ),
      bagItemPath
    )
  )

  val emptyFiles = Nil

  def createStorageManifestWith(
    space: StorageSpace = createStorageSpace,
    bagInfo: BagInfo = createBagInfo,
    version: Int = 1,
    locations: List[ObjectLocation] = List(createObjectLocation)
  ): StorageManifest =
    StorageManifest(
      space = space,
      info = bagInfo,
      version = version,
      manifest = BagManifest(
        checksumAlgorithm,
        emptyFiles
      ),
      tagManifest = BagManifest(
        checksumAlgorithm,
        manifestFiles
      ),
      locations = locations.map { StorageLocation(StandardStorageProvider, _) },
      createdDate = Instant.now
    )

  def createStorageManifest: StorageManifest =
    createStorageManifestWith()
}

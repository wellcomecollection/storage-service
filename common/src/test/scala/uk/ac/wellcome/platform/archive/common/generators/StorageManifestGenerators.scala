package uk.ac.wellcome.platform.archive.common.generators

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.{BagInfo, BagVersion}
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  StandardStorageProvider,
  StorageLocation
}
import uk.ac.wellcome.platform.archive.common.storage.models.{
  FileManifest,
  StorageManifest,
  StorageManifestFile,
  StorageSpace
}
import uk.ac.wellcome.platform.archive.common.verify.SHA256
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators

import scala.util.Random

trait StorageManifestGenerators
    extends BagInfoGenerators
    with BagFileGenerators
    with StorageSpaceGenerators
    with ObjectLocationGenerators {

  val checksumAlgorithm = SHA256

  private def createStorageManifestFile: StorageManifestFile = {
    val bagFile = createBagFile
    StorageManifestFile(
      checksum = bagFile.checksum.value,
      name = bagFile.path.value,
      path = bagFile.path.value
    )
  }

  def createStorageManifestWith(
    space: StorageSpace = createStorageSpace,
    bagInfo: BagInfo = createBagInfo,
    version: BagVersion = BagVersion(Random.nextInt),
    locations: List[ObjectLocation] = List(createObjectLocation)
  ): StorageManifest =
    StorageManifest(
      space = space,
      info = bagInfo,
      version = version,
      manifest = FileManifest(
        checksumAlgorithm,
        files = Seq(
          createStorageManifestFile,
          createStorageManifestFile,
          createStorageManifestFile
        )
      ),
      tagManifest = FileManifest(
        checksumAlgorithm,
        files = Seq(
          createStorageManifestFile,
          createStorageManifestFile,
          createStorageManifestFile
        )
      ),
      locations = locations.map { StorageLocation(StandardStorageProvider, _) },
      createdDate = Instant.now
    )

  def createStorageManifest: StorageManifest =
    createStorageManifestWith()
}

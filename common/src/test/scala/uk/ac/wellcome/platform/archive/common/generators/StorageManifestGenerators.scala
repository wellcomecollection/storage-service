package uk.ac.wellcome.platform.archive.common.generators

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.{BagDigestFile, BagInfo, BagItemPath}
import uk.ac.wellcome.platform.archive.common.ingests.models.{StandardStorageProvider, StorageLocation}
import uk.ac.wellcome.platform.archive.common.storage.models
import uk.ac.wellcome.platform.archive.common.storage.models.{ChecksumAlgorithm, FileManifest, StorageManifest, StorageSpace}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3

trait StorageManifestGenerators
    extends BagInfoGenerators
    with StorageSpaceGenerators
    with S3 {
  def createStorageManifestWith(
    space: StorageSpace = createStorageSpace,
    bagInfo: BagInfo = createBagInfo,
    checksumAlgorithm: String = "sha256",
    accessLocation: ObjectLocation = createObjectLocation,
    archiveLocations: List[ObjectLocation] = List.empty
  ): StorageManifest =
    models.StorageManifest(
      space = space,
      info = bagInfo,
      manifest = FileManifest(
        checksumAlgorithm = ChecksumAlgorithm(checksumAlgorithm),
        files = Nil
      ),
      tagManifest = FileManifest(
        checksumAlgorithm = ChecksumAlgorithm(checksumAlgorithm),
        files = List(BagDigestFile("a", BagItemPath("bag-info.txt")))
      ),
      StorageLocation(StandardStorageProvider, accessLocation),
      archiveLocations.map(StorageLocation(StandardStorageProvider, _)),
      Instant.now
    )

  def createStorageManifest: StorageManifest =
    createStorageManifestWith()
}

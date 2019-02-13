package uk.ac.wellcome.platform.archive.registrar.generators

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.generators.{
  BagInfoGenerators,
  StorageSpaceGenerators
}
import uk.ac.wellcome.platform.archive.common.models.bagit.{
  BagDigestFile,
  BagInfo,
  BagItemPath
}
import uk.ac.wellcome.platform.archive.common.progress.models.{
  StandardStorageProvider,
  StorageLocation
}
import uk.ac.wellcome.platform.archive.registrar.common.models._
import uk.ac.wellcome.storage.ObjectLocation

trait StorageManifestGenerators
    extends BagInfoGenerators
    with StorageSpaceGenerators {
  def createStorageManifestWith(
    bagInfo: BagInfo = createBagInfo,
    checksumAlgorithm: String = "sha256",
    accessLocation: ObjectLocation = ObjectLocation("bucket", "path"),
    archiveLocations: List[ObjectLocation] = List.empty
  ): StorageManifest =
    StorageManifest(
      space = createStorageSpace,
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
}

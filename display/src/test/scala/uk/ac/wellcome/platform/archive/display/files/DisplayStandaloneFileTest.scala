package uk.ac.wellcome.platform.archive.display.files

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.common.verify.MD5

class DisplayStandaloneFileTest extends FunSpec with Matchers with StorageManifestGenerators {
  it("creates a DisplayStandaloneFile from a storage manifest and file") {
    val storageManifest = createStorageManifestWithFileCount(fileCount = 1)

    val file = storageManifest.manifest.files.head

    val displayFile = DisplayStandaloneFile(file = file, storageManifest = storageManifest)

    displayFile.checksum shouldBe s"sha256:${file.checksum}"
    displayFile.name shouldBe file.name
    displayFile.path shouldBe file.path
    displayFile.size shouldBe file.size

    displayFile.bag shouldBe DisplayAssociatedBag(storageManifest)
  }

  it("uses the correct checksum algorithm") {
    val storageManifest = createStorageManifestWithFileCount(fileCount = 1)

    val md5Manifest = storageManifest.copy(
      manifest = storageManifest.manifest.copy(
        checksumAlgorithm = MD5
      )
    )

    val file = storageManifest.manifest.files.head

    val displayFile = DisplayStandaloneFile(file = file, storageManifest = md5Manifest)

    displayFile.checksum shouldBe s"md5:${file.checksum}"
  }

  it("blocks creating a file from a different manifest") {
    val storageManifest = createStorageManifestWithFileCount(fileCount = 0)

    val file = createStorageManifestFile

    val thrown = intercept[IllegalArgumentException] {
      DisplayStandaloneFile(file = file, storageManifest = storageManifest)
    }

    thrown.getMessage shouldBe s"requirement failed: File ${file.name} is not part of storage manifest ${storageManifest.idWithVersion}"
  }
}

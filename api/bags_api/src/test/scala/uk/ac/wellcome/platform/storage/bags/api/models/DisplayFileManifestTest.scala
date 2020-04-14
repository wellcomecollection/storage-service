package uk.ac.wellcome.platform.storage.bags.api.models

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.common.storage.models.FileManifest
import uk.ac.wellcome.platform.archive.common.verify.{MD5, SHA1, SHA256, SHA512}

class DisplayFileManifestTest extends FunSpec with Matchers with StorageManifestGenerators {
  it("converts a manifest to a DisplayManifest") {
    val manifest = FileManifest(
      checksumAlgorithm = chooseFrom(Seq(MD5, SHA1, SHA256, SHA512)),
      files = Seq(
        createStorageManifestFile,
        createStorageManifestFile,
        createStorageManifestFile
      )
    )

    val displayManifest = DisplayFileManifest(manifest)

    displayManifest.checksumAlgorithm shouldBe manifest.checksumAlgorithm.toString

    val expectedFiles = manifest.files.map { file =>
      DisplayFile(
        checksum = file.checksum.value,
        name = file.name,
        path = file.path,
        size = file.size
      )
    }

    displayManifest.files should contain theSameElementsAs expectedFiles
  }
}

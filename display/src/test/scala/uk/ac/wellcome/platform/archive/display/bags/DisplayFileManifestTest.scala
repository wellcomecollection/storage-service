package uk.ac.wellcome.platform.archive.display.bags

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.common.storage.models.{
  FileManifest,
  StorageManifestFile
}
import uk.ac.wellcome.platform.archive.common.verify._

class DisplayFileManifestTest
    extends AnyFunSpec
    with Matchers
    with StorageManifestGenerators {
  it("converts a manifest to a DisplayManifest") {
    val manifest = createFileManifestWith(
      files = Seq(
        createStorageManifestFile,
        createStorageManifestFile,
        createStorageManifestFile
      )
    )

    val displayFileManifest = DisplayFileManifest(manifest)

    displayFileManifest.checksumAlgorithm shouldBe manifest.checksumAlgorithm.toString

    val expectedFiles = manifest.files.map { file =>
      DisplayFile(
        checksum = file.checksum.value,
        name = file.name,
        path = file.path,
        size = file.size
      )
    }

    displayFileManifest.files should contain theSameElementsAs expectedFiles
  }

  it("sorts the files in the manifest by name order") {
    val manifest = createFileManifestWith(
      files = Seq(
        createStorageManifestFileWithName("data/bob.txt"),
        createStorageManifestFileWithName("data/alice.txt"),
        createStorageManifestFileWithName("data/carol.txt")
      )
    )

    val displayFileManifest = DisplayFileManifest(manifest)

    displayFileManifest.files
      .map { _.name } shouldBe Seq(
      "data/alice.txt",
      "data/bob.txt",
      "data/carol.txt"
    )
  }

  private def createStorageManifestFileWithName(
    name: String
  ): StorageManifestFile =
    createStorageManifestFile.copy(name = name)

  private def createFileManifestWith(
    files: Seq[StorageManifestFile]
  ): FileManifest =
    FileManifest(
      checksumAlgorithm = chooseFrom(Seq(MD5, SHA1, SHA256, SHA512)),
      files = files
    )
}

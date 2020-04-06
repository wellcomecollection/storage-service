package uk.ac.wellcome.platform.archive.bag_indexer.models

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagVersion,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.common.storage.models.{
  StorageManifestFile,
  StorageSpace
}
import uk.ac.wellcome.platform.archive.common.verify.ChecksumValue

class IndexedFileTest
    extends FunSpec
    with Matchers
    with StorageManifestGenerators {
  it("creates an IndexedFile from a StorageManifest and a StorageManifestFile") {
    val storageManifestFile = StorageManifestFile(
      checksum = ChecksumValue("2aae6c35c94fcfb415dbe95f408b9ce91ee846ed"),
      name = "alice.txt",
      path = "v1/alice.txt",
      size = 1000
    )

    val storageManifest = createStorageManifestWith(
      space = StorageSpace("digitised"),
      bagInfo = createBagInfoWith(
        externalIdentifier = ExternalIdentifier("b1234")
      ),
      manifestFiles = Seq(storageManifestFile),
      version = BagVersion(1)
    )

    val indexedFile = IndexedFile(
      manifest = storageManifest,
      file = storageManifestFile
    )

    indexedFile shouldBe IndexedFile(
      bucket = storageManifest.location.prefix.namespace,
      path = s"${storageManifest.location.prefix.path}/v1/alice.txt",
      name = "alice.txt",
      size = 1000,
      createdDate = storageManifest.createdDate.toString,
      checksum = IndexedChecksum(
        algorithm = "SHA-256",
        value = "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed"
      ),
      bag = BagPointer(
        space = "digitised",
        externalIdentifier = "b1234",
        versions = Seq("v1")
      )
    )
  }
}

package uk.ac.wellcome.platform.archive.common.storage.services

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.bagit.models.BagPath
import uk.ac.wellcome.platform.archive.common.fixtures.{
  BagLocationFixtures,
  FileEntry
}
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  InfrequentAccessStorageProvider,
  StorageLocation
}
import uk.ac.wellcome.platform.archive.common.verify.SHA256
import uk.ac.wellcome.storage.StorageBackend
import uk.ac.wellcome.storage.fixtures.S3
import uk.ac.wellcome.storage.memory.MemoryStorageBackend

class StorageManifestServiceTest
    extends FunSpec
    with Matchers
    with BagLocationFixtures
    with S3 {

  def createBackend = new MemoryStorageBackend()

  def createStorageManifestService(
    storageBackend: StorageBackend = createBackend): StorageManifestService =
    new StorageManifestService()(storageBackend)

  it("returns a StorageManifest if reading a bag location succeeds") {
    val backend = createBackend
    val service = createStorageManifestService(backend)

    val bagInfo = createBagInfo
    withBag(backend, bagInfo = bagInfo) {
      case (bagRootLocation, storageSpace) =>
        val maybeManifest = service.retrieve(
          bagRootLocation = bagRootLocation,
          storageSpace = storageSpace
        )

        val storageManifest = maybeManifest.get

        storageManifest.space shouldBe storageSpace
        storageManifest.info shouldBe bagInfo

        storageManifest.manifest.checksumAlgorithm shouldBe SHA256
        storageManifest.manifest.files should have size 1

        storageManifest.tagManifest.checksumAlgorithm shouldBe SHA256
        storageManifest.tagManifest.files should have size 3
        val actualFiles = storageManifest.tagManifest.files.map { _.path }
        val expectedFiles = List(
          BagPath("manifest-sha256.txt"),
          BagPath("bag-info.txt"),
          BagPath("bagit.txt")
        )
        actualFiles should contain theSameElementsAs expectedFiles

        storageManifest.locations shouldBe List(
          StorageLocation(
            provider = InfrequentAccessStorageProvider,
            location = bagRootLocation
          )
        )
    }
  }

  describe("returns a Left upon error") {
    it("if no files are at the BagLocation") {
      val service = createStorageManifestService()

      val maybeManifest = service.retrieve(
        bagRootLocation = createObjectLocation,
        storageSpace = createStorageSpace
      )

      val err = maybeManifest.failed.get

      err shouldBe a[RuntimeException]
      err.getMessage should startWith("Error getting bag info: Nothing at ")
      err.getMessage should endWith("/bag-info.txt")
    }

    it("the bag-info.txt file is missing") {
      val backend = createBackend
      val service = createStorageManifestService(backend)

      withBag(backend) {
        case (bagRootLocation, storageSpace) =>
          backend.storage = backend.storage - bagRootLocation.join(
            "bag-info.txt")

          val maybeManifest = service.retrieve(
            bagRootLocation = bagRootLocation,
            storageSpace = storageSpace
          )

          val err = maybeManifest.failed.get

          err shouldBe a[RuntimeException]
          err.getMessage should startWith("Error getting bag info: Nothing at ")
          err.getMessage should endWith("/bag-info.txt")
      }
    }

    it("the manifest file is missing") {
      val backend = createBackend
      val service = createStorageManifestService(backend)

      withBag(backend) {
        case (bagRootLocation, storageSpace) =>
          backend.storage = backend.storage - bagRootLocation.join(
            "manifest-sha256.txt")

          val maybeManifest = service.retrieve(
            bagRootLocation = bagRootLocation,
            storageSpace = storageSpace
          )

          val err = maybeManifest.failed.get

          err shouldBe a[RuntimeException]
          err.getMessage should startWith(
            "Error getting file manifest: Nothing at ")
          err.getMessage should endWith("/manifest-sha256.txt")
      }
    }

    it("if the manifest.txt file has a badly formatted line") {
      val backend = createBackend
      val service = createStorageManifestService(backend)

      withBag(
        backend,
        createDataManifest =
          _ => Some(FileEntry("manifest-sha256.txt", "bleeergh!"))) {
        case (bagRootLocation, storageSpace) =>
          val maybeManifest = service.retrieve(
            bagRootLocation = bagRootLocation,
            storageSpace = storageSpace
          )

          val err = maybeManifest.failed.get

          err shouldBe a[RuntimeException]
          err.getMessage shouldBe "Error getting file manifest: Failed to parse: List(bleeergh!)"
      }
    }

    it("the tag manifest file is missing") {
      val backend = createBackend
      val service = createStorageManifestService(backend)

      withBag(backend) {
        case (bagRootLocation, storageSpace) =>
          backend.storage = backend.storage - bagRootLocation.join(
            "tagmanifest-sha256.txt")

          val maybeManifest = service.retrieve(
            bagRootLocation = bagRootLocation,
            storageSpace = storageSpace
          )

          val err = maybeManifest.failed.get

          err shouldBe a[RuntimeException]
          err.getMessage should startWith(
            "Error getting tag manifest: Nothing at ")
          err.getMessage should endWith("/tagmanifest-sha256.txt")
      }
    }

    it("if the tag manifest file has a badly formatted line") {
      val backend = createBackend
      val service = createStorageManifestService(backend)

      withBag(
        backend,
        createTagManifest =
          _ => Some(FileEntry("tagmanifest-sha256.txt", "blaaargh!"))) {
        case (bagRootLocation, storageSpace) =>
          val maybeManifest = service.retrieve(
            bagRootLocation = bagRootLocation,
            storageSpace = storageSpace
          )

          val err = maybeManifest.failed.get

          err shouldBe a[RuntimeException]
          err.getMessage shouldBe "Error getting tag manifest: Failed to parse: List(blaaargh!)"
      }
    }
  }
}

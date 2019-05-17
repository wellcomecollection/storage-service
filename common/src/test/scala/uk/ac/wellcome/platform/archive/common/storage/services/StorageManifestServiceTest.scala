package uk.ac.wellcome.platform.archive.common.storage.services

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
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

class StorageManifestServiceTest
    extends FunSpec
    with Matchers
    with BagLocationFixtures {

  def withStorageManifestService[R](
    testWith: TestWith[StorageManifestService, R]): R =
    testWith(new StorageManifestService())

  it("returns a StorageManifest if reading a bag location succeeds") {
    withLocalS3Bucket { bucket =>
      val bagInfo = createBagInfo
      withBag(bucket, bagInfo = bagInfo) {
        case (bagRootLocation, storageSpace) =>
          withStorageManifestService { service =>
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
    }
  }

  describe("returns a Left upon error") {
    it("if no files are at the BagLocation") {
      withLocalS3Bucket { bucket =>
        withStorageManifestService { service =>
          val maybeManifest = service.retrieve(
            bagRootLocation = createObjectLocationWith(bucket),
            storageSpace = createStorageSpace
          )

          val err = maybeManifest.failed.get

          err shouldBe a[RuntimeException]
          err.getMessage should include("The specified key does not exist.")
        }
      }
    }

    it("the bag-info.txt file is missing") {
      withLocalS3Bucket { bucket =>
        withBag(bucket) {
          case (bagRootLocation, storageSpace) =>
            s3Client.deleteObject(
              bagRootLocation.namespace,
              bagRootLocation.key + "/bag-info.txt"
            )

            withStorageManifestService { service =>
              val maybeManifest = service.retrieve(
                bagRootLocation = bagRootLocation,
                storageSpace = storageSpace
              )

              val err = maybeManifest.failed.get

              err shouldBe a[RuntimeException]
              err.getMessage should include("The specified key does not exist.")

            }
        }
      }
    }

    it("the manifest.txt file is missing") {
      withLocalS3Bucket { bucket =>
        withBag(bucket) {
          case (bagRootLocation, storageSpace) =>
            s3Client.deleteObject(
              bagRootLocation.namespace,
              bagRootLocation.key + "/manifest-sha256.txt"
            )

            withStorageManifestService { service =>
              val maybeManifest = service.retrieve(
                bagRootLocation = bagRootLocation,
                storageSpace = storageSpace
              )

              val err = maybeManifest.failed.get

              err shouldBe a[RuntimeException]
              err.getMessage should include("The specified key does not exist.")
            }
        }
      }
    }

    it("if the manifest.txt file has a badly formatted line") {
      withLocalS3Bucket { bucket =>
        withBag(
          bucket,
          createDataManifest =
            _ => Some(FileEntry("manifest-sha256.txt", "bleeergh!"))) {
          case (bagRootLocation, storageSpace) =>
            withStorageManifestService { service =>
              val maybeManifest = service.retrieve(
                bagRootLocation = bagRootLocation,
                storageSpace = storageSpace
              )

              val err = maybeManifest.failed.get

              err shouldBe a[RuntimeException]
              err.getMessage shouldBe "Error getting file manifest: Failed to parse: List(bleeergh!)"
            }
        }

      }
    }

    it("the tagmanifest.txt file is missing") {
      withLocalS3Bucket { bucket =>
        withBag(bucket) {
          case (bagRootLocation, storageSpace) =>
            s3Client.deleteObject(
              bagRootLocation.namespace,
              bagRootLocation.key + "/tagmanifest-sha256.txt"
            )

            withStorageManifestService { service =>
              val maybeManifest = service.retrieve(
                bagRootLocation = bagRootLocation,
                storageSpace = storageSpace
              )

              val err = maybeManifest.failed.get

              err shouldBe a[RuntimeException]
              err.getMessage should include("The specified key does not exist.")
            }
        }
      }
    }

    it("if the tag-manifest.txt file has a badly formatted line") {
      withLocalS3Bucket { bucket =>
        withBag(
          bucket,
          createTagManifest =
            _ => Some(FileEntry("tagmanifest-sha256.txt", "blaaargh!"))) {
          case (bagRootLocation, storageSpace) =>
            withStorageManifestService { service =>
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
  }
}

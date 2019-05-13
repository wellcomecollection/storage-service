package uk.ac.wellcome.platform.archive.common.storage.services

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.fixtures.{BagLocationFixtures, FileEntry}
import uk.ac.wellcome.platform.archive.common.ingests.models.{InfrequentAccessStorageProvider, StorageLocation}
import uk.ac.wellcome.platform.archive.common.verify.ChecksumAlgorithm
import uk.ac.wellcome.storage.ObjectLocation


class StorageManifestServiceTest
  extends FunSpec
    with Matchers
    with ScalaFutures
    with BagLocationFixtures {

  def withStorageManifestService[R](root: ObjectLocation)(testWith: TestWith[StorageManifestService, R]): R = {

    val service = new StorageManifestService()

    testWith(service)
  }

  it("returns a StorageManifest if reading a bag location succeeds") {
    withLocalS3Bucket { bucket =>
      val bagInfo = createBagInfo
      withBag(bucket, bagInfo = bagInfo) {
        case (root, space) =>
          withStorageManifestService(root) { service =>

            val maybeManifest = service.createManifest(
              root = root,
              space = space
            )

            val storageManifest = maybeManifest.get

            storageManifest.space shouldBe space
            storageManifest.info shouldBe bagInfo

            storageManifest.manifest.checksumAlgorithm shouldBe ChecksumAlgorithm(
              "sha256")
            storageManifest.manifest.files should have size 1

            storageManifest.tagManifest.checksumAlgorithm shouldBe ChecksumAlgorithm(
              "sha256")
            storageManifest.tagManifest.files should have size 3
            val actualFiles =
              storageManifest.tagManifest.files
                .map {
                  _.path.toString
                }
            val expectedFiles = List(
              "manifest-sha256.txt",
              "bag-info.txt",
              "bagit.txt"
            )
            actualFiles should contain theSameElementsAs expectedFiles

            storageManifest.locations shouldBe List(
              StorageLocation(
                provider = InfrequentAccessStorageProvider,
                location = root
              )
            )
          }
      }
    }
  }

  describe("returns a Left upon error") {
    it("if no files are at the BagLocation") {
      withLocalS3Bucket { bucket =>

        val root = createObjectLocationWith(bucket)

        withStorageManifestService(root) { service =>


          val maybeManifest = service.createManifest(
            root = createObjectLocationWith(bucket),
            space = createStorageSpace
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
          case (root, storageSpace) =>
            s3Client.deleteObject(
              root.namespace,
              root.key + "/bag-info.txt"
            )

            withStorageManifestService(root) { service =>

              val maybeManifest = service.createManifest(
                root = root,
                space = storageSpace
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
          case (root, space) =>
            s3Client.deleteObject(
              root.namespace,
              root.key + "/manifest-sha256.txt"
            )

            withStorageManifestService(root) { service =>

              val maybeManifest =
                service.createManifest(root, space)

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
          case (root, space) =>

            withStorageManifestService(root) { service =>

              val maybeManifest = service.createManifest(
                root = root,
                space = space
              )

              val err = maybeManifest.failed.get

              err shouldBe a[RuntimeException]
              err.getMessage shouldBe "Line <<bleeergh!>> is incorrectly formatted!"
            }
        }

      }
    }

    it("the tagmanifest.txt file is missing") {
      withLocalS3Bucket { bucket =>
        withBag(bucket) {
          case (root, space) =>
            s3Client.deleteObject(
              root.namespace,
              root.key + "/tagmanifest-sha256.txt"
            )

            withStorageManifestService(root) { service =>

              val maybeManifest = service.createManifest(
                root = root,
                space = space
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
          case (root, space) =>

            withStorageManifestService(root) { service =>

              val maybeManifest = service.createManifest(
                root = root,
                space = space
              )

              val err = maybeManifest.failed.get


              err shouldBe a[RuntimeException]
              err.getMessage shouldBe "Line <<blaaargh!>> is incorrectly formatted!"
            }
        }
      }
    }
  }
}

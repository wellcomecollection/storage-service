package uk.ac.wellcome.platform.archive.common.storage.services

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.bagit.models.BagLocation
import uk.ac.wellcome.platform.archive.common.fixtures.{
  BagLocationFixtures,
  FileEntry
}
import uk.ac.wellcome.platform.archive.common.generators.BagRequestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  InfrequentAccessStorageProvider,
  StorageLocation
}
import uk.ac.wellcome.platform.archive.common.storage.models.ChecksumAlgorithm
import uk.ac.wellcome.storage.fixtures.S3

import scala.concurrent.ExecutionContext.Implicits.global

class StorageManifestServiceTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with BagLocationFixtures
    with BagRequestGenerators
    with S3 {

  implicit val _s3Client = s3Client

  val service = new StorageManifestService()

  it("returns a StorageManifest if reading a bag location succeeds") {
    withLocalS3Bucket { bucket =>
      val bagInfo = createBagInfo
      withBag(bucket, bagInfo = bagInfo) { bagLocation =>
        val bagRequest = createBagRequestWith(bagLocation)

        val future = service.createManifest(bagRequest.bagLocation)

        whenReady(future) { storageManifest =>
          storageManifest.space shouldBe bagLocation.storageSpace
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
              location = bagLocation.objectLocation
            )
          )
        }
      }
    }
  }

  describe("returns a Left upon error") {
    it("if no files are at the BagLocation") {
      withLocalS3Bucket { bucket =>
        val bagLocation = BagLocation(
          storageNamespace = bucket.name,
          storagePrefix = Some("archive"),
          storageSpace = createStorageSpace,
          bagPath = randomBagPath
        )

        val bagRequest = createBagRequestWith(bagLocation)

        val future = service.createManifest(bagRequest.bagLocation)

        whenReady(future.failed) { err =>
          err shouldBe a[RuntimeException]
          err.getMessage should include("The specified key does not exist.")
        }
      }
    }

    it("the bag-info.txt file is missing") {
      withLocalS3Bucket { bucket =>
        withBag(bucket) { bagLocation =>
          s3Client.deleteObject(
            bucket.name,
            bagLocation.completePath + "/bag-info.txt"
          )

          val bagRequest = createBagRequestWith(bagLocation)

          val future = service.createManifest(bagRequest.bagLocation)

          whenReady(future.failed) { err =>
            err shouldBe a[RuntimeException]
            err.getMessage should include("The specified key does not exist.")
          }
        }
      }
    }

    it("the manifest.txt file is missing") {
      withLocalS3Bucket { bucket =>
        withBag(bucket) { bagLocation =>
          s3Client.deleteObject(
            bucket.name,
            bagLocation.completePath + "/manifest-sha256.txt"
          )

          val bagRequest = createBagRequestWith(bagLocation)

          val future = service.createManifest(bagRequest.bagLocation)

          whenReady(future.failed) { err =>
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
          bagLocation =>
            val bagRequest = createBagRequestWith(bagLocation)

            val future = service.createManifest(bagRequest.bagLocation)

            whenReady(future.failed) { err =>
              err shouldBe a[RuntimeException]
              err.getMessage shouldBe "Line <<bleeergh!>> is incorrectly formatted!"
            }
        }
      }
    }

    it("the tagmanifest.txt file is missing") {
      withLocalS3Bucket { bucket =>
        withBag(bucket) { bagLocation =>
          s3Client.deleteObject(
            bucket.name,
            bagLocation.completePath + "/tagmanifest-sha256.txt"
          )

          val bagRequest = createBagRequestWith(bagLocation)

          val future = service.createManifest(bagRequest.bagLocation)

          whenReady(future.failed) { err =>
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
          bagLocation =>
            val bagRequest = createBagRequestWith(bagLocation)

            val future = service.createManifest(bagRequest.bagLocation)

            whenReady(future.failed) { err =>
              err shouldBe a[RuntimeException]
              err.getMessage shouldBe "Line <<blaaargh!>> is incorrectly formatted!"
            }
        }
      }
    }
  }
}

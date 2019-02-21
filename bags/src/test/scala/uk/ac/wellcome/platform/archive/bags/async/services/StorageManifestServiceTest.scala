package uk.ac.wellcome.platform.archive.bags.async.services

import com.amazonaws.services.s3.model.AmazonS3Exception
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.bags.async.generators.BagManifestUpdateGenerators
import uk.ac.wellcome.platform.archive.bags.common.models.ChecksumAlgorithm
import uk.ac.wellcome.platform.archive.common.fixtures.{BagLocationFixtures, FileEntry}
import uk.ac.wellcome.platform.archive.common.models.bagit
import uk.ac.wellcome.platform.archive.common.models.bagit.BagLocation
import uk.ac.wellcome.platform.archive.common.progress.models.{InfrequentAccessStorageProvider, StorageLocation}
import uk.ac.wellcome.storage.fixtures.S3

import scala.concurrent.ExecutionContext.Implicits.global

class StorageManifestServiceTest extends FunSpec with Matchers with ScalaFutures with BagLocationFixtures with BagManifestUpdateGenerators with S3 {

  val service = new StorageManifestService(s3Client)

  it("returns a StorageManifest if reading a bag location succeeds") {
    withLocalS3Bucket { bucket =>
      val bagInfo = createBagInfo
      withBag(bucket, bagInfo = bagInfo, storagePrefix = "archive") {
        archiveBagLocation =>
          withBag(bucket, bagInfo = bagInfo, storagePrefix = "access") {
            accessBagLocation =>
              val bagManifestUpdate = createBagManifestUpdateWith(
                archiveBagLocation = archiveBagLocation,
                accessBagLocation = accessBagLocation
              )

              val future = service.createManifest(bagManifestUpdate)

              whenReady(future) { result =>
                result.isRight shouldBe true
                val storageManifest = result.right.get

                storageManifest.space shouldBe archiveBagLocation.storageSpace
                storageManifest.info shouldBe bagInfo

                storageManifest.manifest.checksumAlgorithm shouldBe ChecksumAlgorithm("sha256")
                storageManifest.manifest.files should have size 1

                storageManifest.tagManifest.checksumAlgorithm shouldBe ChecksumAlgorithm("sha256")
                storageManifest.tagManifest.files should have size 3
                val actualFiles =
                  storageManifest.tagManifest.files
                    .map { _.path.toString }
                val expectedFiles = List(
                  "manifest-sha256.txt",
                  "bag-info.txt",
                  "bagit.txt"
                )
                actualFiles should contain theSameElementsAs expectedFiles

                storageManifest.accessLocation shouldBe StorageLocation(
                  provider = InfrequentAccessStorageProvider,
                  location = accessBagLocation.objectLocation
                )
                storageManifest.archiveLocations shouldBe List(
                  StorageLocation(
                    provider = InfrequentAccessStorageProvider,
                    location = archiveBagLocation.objectLocation
                  )
                )
              }
          }
      }
    }
  }

  describe("returns a Left upon error") {
    it("if no files are at the BagLocation") {
      withLocalS3Bucket { bucket =>
        val bagLocation = bagit.BagLocation(
          storageNamespace = bucket.name,
          storagePrefix = Some("archive"),
          storageSpace = createStorageSpace,
          bagPath = randomBagPath
        )

        val bagManifestUpdate = createBagManifestUpdateFor(bagLocation)

        val future = service.createManifest(bagManifestUpdate)

        whenReady(future) { result =>
          result.isLeft shouldBe true
          val err = result.left.get
          err shouldBe a[AmazonS3Exception]
          err.getMessage should startWith("The specified key does not exist.")
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

          val bagManifestUpdate = createBagManifestUpdateFor(bagLocation)

          val future = service.createManifest(bagManifestUpdate)

          whenReady(future) { result =>
            result.isLeft shouldBe true
            val err = result.left.get
            err shouldBe a[AmazonS3Exception]
            err.getMessage should startWith("The specified key does not exist.")
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

          val bagManifestUpdate = createBagManifestUpdateFor(bagLocation)

          val future = service.createManifest(bagManifestUpdate)

          whenReady(future) { result =>
            result.isLeft shouldBe true
            val err = result.left.get
            err shouldBe a[AmazonS3Exception]
            err.getMessage should startWith("The specified key does not exist.")
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
            val bagManifestUpdate = createBagManifestUpdateFor(bagLocation)

            val future = service.createManifest(bagManifestUpdate)

            whenReady(future) { result =>
              result.isLeft shouldBe true
              val err = result.left.get
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

          val bagManifestUpdate = createBagManifestUpdateFor(bagLocation)

          val future = service.createManifest(bagManifestUpdate)

          whenReady(future) { result =>
            result.isLeft shouldBe true
            val err = result.left.get
            err shouldBe a[AmazonS3Exception]
            err.getMessage should startWith("The specified key does not exist.")
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
            val bagManifestUpdate = createBagManifestUpdateFor(bagLocation)

            val future = service.createManifest(bagManifestUpdate)

            whenReady(future) { result =>
              result.isLeft shouldBe true
              val err = result.left.get
              err shouldBe a[RuntimeException]
              err.getMessage shouldBe "Line <<blaaargh!>> is incorrectly formatted!"
            }
        }
      }
    }

    def createBagManifestUpdateFor(bagLocation: BagLocation) =
      createBagManifestUpdateWith(
        archiveBagLocation = bagLocation,
        accessBagLocation = bagLocation
      )
  }
}

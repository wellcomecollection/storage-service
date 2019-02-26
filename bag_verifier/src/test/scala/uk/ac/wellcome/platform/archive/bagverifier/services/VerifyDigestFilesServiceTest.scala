package uk.ac.wellcome.platform.archive.bagverifier.services

import org.apache.commons.codec.digest.MessageDigestAlgorithms
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.fixtures.{BagLocationFixtures, FileEntry}
import uk.ac.wellcome.platform.archive.common.services.StorageManifestService
import uk.ac.wellcome.storage.fixtures.S3

import scala.concurrent.ExecutionContext.Implicits.global

class VerifyDigestFilesServiceTest extends FunSpec with Matchers with ScalaFutures with S3 with BagLocationFixtures {

  implicit val _ = s3Client

  val service = new VerifyDigestFilesService(
    storageManifestService = new StorageManifestService(),
    s3Client = s3Client,
    algorithm = MessageDigestAlgorithms.SHA_256
  )

  val dataFileCount = 3

  // Data files plus manifest-sha256.txt, bagit.txt, bag-info.txt
  val expectedDataFileCount = dataFileCount + 3

  it("passes a bag with correct checksums") {
    withLocalS3Bucket { bucket =>
      withBag(bucket, dataFileCount = dataFileCount) { bagLocation =>
        val future = service.verifyBagLocation(bagLocation)
        whenReady(future) { result =>
          result shouldBe a[BagVerification]
          result.woke should have size expectedDataFileCount
          result.problematicFaves shouldBe Seq.empty
        }
      }
    }
  }

  it("fails a bag with an incorrect checksum in the file manifest") {
    withLocalS3Bucket { bucket =>
      withBag(bucket, dataFileCount = dataFileCount, createDataManifest = dataManifestWithWrongChecksum) { bagLocation =>
        val future = service.verifyBagLocation(bagLocation)
        whenReady(future) { result =>
          result shouldBe a[BagVerification]
          result.woke should have size expectedDataFileCount - 1
          result.problematicFaves should have size 1

          val brokenFile = result.problematicFaves.head
          brokenFile.reason shouldBe a[RuntimeException]
          brokenFile.reason.getMessage should startWith("Checksums do not match:")
        }
      }
    }
  }

  it("fails a bag with an incorrect checksum in the tag manifest") {
    withLocalS3Bucket { bucket =>
      withBag(bucket, dataFileCount = dataFileCount, createTagManifest = tagManifestWithWrongChecksum) { bagLocation =>
        val future = service.verifyBagLocation(bagLocation)
        whenReady(future) { result =>
          result shouldBe a[BagVerification]
          result.woke should have size expectedDataFileCount - 1
          result.problematicFaves should have size 1

          val brokenFile = result.problematicFaves.head
          brokenFile.reason shouldBe a[RuntimeException]
          brokenFile.reason.getMessage should startWith("Checksums do not match:")
        }
      }
    }
  }

  it("fails a bag if the file manifest refers to a non-existent file") {
    def createDataManifestWithExtraFile(dataFiles: List[(String, String)]): Option[FileEntry] =
      createValidDataManifest(dataFiles ++ List(("doesnotexist", "doesnotexist")))

    withLocalS3Bucket { bucket =>
      withBag(bucket, dataFileCount = dataFileCount, createDataManifest = createDataManifestWithExtraFile) { bagLocation =>
        val future = service.verifyBagLocation(bagLocation)
        whenReady(future) { result =>
          result shouldBe a[BagVerification]
          result.woke should have size expectedDataFileCount
          result.problematicFaves should have size 1

          val brokenFile = result.problematicFaves.head
          brokenFile.reason shouldBe a[RuntimeException]
          brokenFile.reason.getMessage should startWith("The specified key does not exist")
        }
      }
    }
  }

  it("fails a bag if the file manifest does not exist") {
    def dontCreateTheDataManifest(dataFiles: List[(String, String)]): Option[FileEntry] = None

    withLocalS3Bucket { bucket =>
      withBag(bucket, createDataManifest = dontCreateTheDataManifest) { bagLocation =>
        val future = service.verifyBagLocation(bagLocation)
        whenReady(future.failed) { err =>
          err shouldBe a[RuntimeException]
          err.getMessage should startWith("Error getting file manifest: The specified key does not exist")
        }
      }
    }
  }

  it("fails a bag if the tag manifest does not exist") {
    def dontCreateTheTagManifest(dataFiles: List[(String, String)]): Option[FileEntry] = None

    withLocalS3Bucket { bucket =>
      withBag(bucket, createTagManifest = dontCreateTheTagManifest) { bagLocation =>
        val future = service.verifyBagLocation(bagLocation)
        whenReady(future.failed) { err =>
          err shouldBe a[RuntimeException]
          err.getMessage should startWith("Error getting tag manifest: The specified key does not exist")
        }
      }
    }
  }
}

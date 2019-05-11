package uk.ac.wellcome.platform.archive.bagverifier.services

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.bagverifier.fixtures.BagVerifierFixtures
import uk.ac.wellcome.platform.archive.bagverifier.models.VerificationSummary
import uk.ac.wellcome.platform.archive.common.fixtures.{
  BagLocationFixtures,
  FileEntry
}
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestFailed,
  IngestStepSucceeded
}

class BagVerifierTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with BagLocationFixtures
    with BagVerifierFixtures {

  val dataFileCount = 3

  val expectedFileCount: Int = dataFileCount + List(
    "manifest-sha256.txt",
    "bagit.txt",
    "bag-info.txt").size

  it("passes a bag with correct checksums") {
    withLocalS3Bucket { bucket =>
      withBag(bucket, dataFileCount = dataFileCount) {
        case (bagRootLocation, _) =>
          withVerifier { verifier =>
            val future = verifier.verify(bagRootLocation)

            whenReady(future) { result =>
              result shouldBe a[IngestStepSucceeded[_]]

              val summary = result.summary

              summary.successfulVerifications should have size expectedFileCount
              summary.failedVerifications shouldBe Seq.empty
            }
          }
      }
    }
  }

  it("fails a bag with an incorrect checksum in the file manifest") {
    withLocalS3Bucket { bucket =>
      withBag(
        bucket,
        dataFileCount = dataFileCount,
        createDataManifest = dataManifestWithWrongChecksum) {
        case (bagRootLocation, _) =>
          withVerifier { verifier =>
            val future = verifier.verify(bagRootLocation)

            whenReady(future) { result =>
              result shouldBe a[IngestFailed[_]]

              val summary = result.summary
              summary.successfulVerifications should have size expectedFileCount - 1
              summary.failedVerifications should have size 1

              val brokenFile = summary.failedVerifications.head
              brokenFile.error shouldBe a[RuntimeException]
              brokenFile.error.getMessage should startWith(
                "Checksums do not match:")
            }
          }
      }
    }
  }

  it("fails a bag with an incorrect checksum in the tag manifest") {
    withLocalS3Bucket { bucket =>
      withBag(
        bucket,
        dataFileCount = dataFileCount,
        createTagManifest = tagManifestWithWrongChecksum) {
        case (bagRootLocation, _) =>
          withVerifier { verifier =>
            val future = verifier.verify(bagRootLocation)
            whenReady(future) { result =>
              result shouldBe a[IngestFailed[_]]

              val summary = result.summary

              summary.successfulVerifications should have size expectedFileCount - 1
              summary.failedVerifications should have size 1

              val brokenFile = summary.failedVerifications.head
              brokenFile.error shouldBe a[RuntimeException]
              brokenFile.error.getMessage should startWith(
                "Checksums do not match:")
            }
          }
      }
    }
  }

  it("fails a bag if the file manifest refers to a non-existent file") {
    def createDataManifestWithExtraFile(
      dataFiles: List[(String, String)]): Option[FileEntry] =
      createValidDataManifest(
        dataFiles ++ List(("doesnotexist", "doesnotexist")))

    withLocalS3Bucket { bucket =>
      withBag(
        bucket,
        dataFileCount = dataFileCount,
        createDataManifest = createDataManifestWithExtraFile) {
        case (bagRootLocation, _) =>
          withVerifier { verifier =>
            val future = verifier.verify(bagRootLocation)
            whenReady(future) { result =>
              result shouldBe a[IngestFailed[_]]

              val summary = result.summary

              summary.successfulVerifications should have size expectedFileCount
              summary.failedVerifications should have size 1

              val brokenFile = summary.failedVerifications.head
              brokenFile.error shouldBe a[RuntimeException]
              brokenFile.error.getMessage should startWith(
                "The specified key does not exist")
            }
          }
      }
    }
  }

  it("fails a bag if the file manifest does not exist") {
    def dontCreateTheDataManifest(
      dataFiles: List[(String, String)]): Option[FileEntry] = None

    withLocalS3Bucket { bucket =>
      withBag(bucket, createDataManifest = dontCreateTheDataManifest) {
        case (bagRootLocation, _) =>
          withVerifier { verifier =>
            val future = verifier.verify(bagRootLocation)

            whenReady(future) { result =>
              result shouldBe a[IngestFailed[_]]
              val err = result
                .asInstanceOf[IngestFailed[VerificationSummary]]
                .e

              err shouldBe a[RuntimeException]
              err.getMessage should startWith("Error getting file manifest")
            }
          }
      }
    }
  }

  it("fails a bag if the tag manifest does not exist") {
    def dontCreateTheTagManifest(
      dataFiles: List[(String, String)]): Option[FileEntry] = None

    withLocalS3Bucket { bucket =>
      withBag(bucket, createTagManifest = dontCreateTheTagManifest) {
        case (bagRootLocation, _) =>
          withVerifier { verifier =>
            val future = verifier.verify(bagRootLocation)

            whenReady(future) { result =>
              result shouldBe a[IngestFailed[_]]
              val err = result
                .asInstanceOf[IngestFailed[VerificationSummary]]
                .e

              err shouldBe a[RuntimeException]
              err.getMessage should startWith("Error getting tag manifest")
            }
          }
      }
    }
  }
}

package uk.ac.wellcome.platform.archive.bagverifier.services

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers, OptionValues, TryValues}
import uk.ac.wellcome.platform.archive.bagverifier.fixtures.BagVerifierFixtures
import uk.ac.wellcome.platform.archive.bagverifier.models.{
  VerificationFailureSummary,
  VerificationIncompleteSummary,
  VerificationSuccessSummary
}
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.bagit.services.BagUnavailable
import uk.ac.wellcome.platform.archive.common.fixtures.{
  FileEntry,
  S3BagLocationFixtures
}
import uk.ac.wellcome.platform.archive.common.storage.LocationNotFound
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestFailed,
  IngestStepSucceeded
}
import uk.ac.wellcome.platform.archive.common.verify.FailedChecksumNoMatch

class BagVerifierTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with TryValues
    with OptionValues
    with S3BagLocationFixtures
    with BagVerifierFixtures {

  type StringTuple = List[(String, String)]

  val dataFileCount = 3

  val expectedFileCount: Int = dataFileCount + List(
    "manifest-sha256.txt",
    "bagit.txt",
    "bag-info.txt").size

  it("passes a bag with correct checksum values ") {
    withLocalS3Bucket { bucket =>
      withS3Bag(bucket, dataFileCount = dataFileCount) {
        case (root, _) =>
          withVerifier { verifier =>
            val ingestStep = verifier.verify(root, externalIdentifier = createExternalIdentifier)
            val result = ingestStep.success.get

            result shouldBe a[IngestStepSucceeded[_]]
            result.summary shouldBe a[VerificationSuccessSummary]

            val summary = result.summary
              .asInstanceOf[VerificationSuccessSummary]
            val verification = summary.verification.value

            verification.locations should have size expectedFileCount
          }
      }
    }
  }

  it("fails a bag with an incorrect checksum in the file manifest") {
    withLocalS3Bucket { bucket =>
      withS3Bag(
        bucket,
        dataFileCount = dataFileCount,
        createDataManifest = dataManifestWithWrongChecksum) {
        case (root, _) =>
          withVerifier { verifier =>
            val ingestStep = verifier.verify(root, externalIdentifier = createExternalIdentifier)
            val result = ingestStep.success.get

            result shouldBe a[IngestFailed[_]]
            result.summary shouldBe a[VerificationFailureSummary]

            val summary = result.summary
              .asInstanceOf[VerificationFailureSummary]
            val verification = summary.verification.value

            verification.success should have size expectedFileCount - 1
            verification.failure should have size 1

            val location = verification.failure.head
            val error = location.e

            error shouldBe a[FailedChecksumNoMatch]
            error.getMessage should include("Checksum values do not match!")
          }
      }
    }
  }

  it("fails a bag with an incorrect checksum in the tag manifest") {
    withLocalS3Bucket { bucket =>
      withS3Bag(
        bucket,
        dataFileCount = dataFileCount,
        createTagManifest = tagManifestWithWrongChecksum) {
        case (root, _) =>
          withVerifier { verifier =>
            val ingestStep = verifier.verify(root, externalIdentifier = createExternalIdentifier)
            val result = ingestStep.success.get

            result shouldBe a[IngestFailed[_]]
            result.summary shouldBe a[VerificationFailureSummary]

            val summary = result.summary
              .asInstanceOf[VerificationFailureSummary]
            val verification = summary.verification.value

            verification.success should have size expectedFileCount - 1
            verification.failure should have size 1

            val location = verification.failure.head
            val error = location.e

            error shouldBe a[FailedChecksumNoMatch]
            error.getMessage should include("Checksum values do not match!")
          }
      }
    }
  }

  it("fails a bag if the file manifest refers to a non-existent file") {
    def createDataManifestWithExtraFile(
      dataFiles: StringTuple): Option[FileEntry] =
      createValidDataManifest(
        dataFiles ++ List(("doesnotexist", "doesnotexist"))
      )

    withLocalS3Bucket { bucket =>
      withS3Bag(
        bucket,
        dataFileCount = dataFileCount,
        createDataManifest = createDataManifestWithExtraFile) {
        case (root, _) =>
          withVerifier { verifier =>
            val ingestStep = verifier.verify(root, externalIdentifier = createExternalIdentifier)
            val result = ingestStep.success.get

            result shouldBe a[IngestFailed[_]]
            result.summary shouldBe a[VerificationFailureSummary]

            val summary = result.summary
              .asInstanceOf[VerificationFailureSummary]
            val verification = summary.verification.value

            verification.success should have size expectedFileCount
            verification.failure should have size 1

            val location = verification.failure.head
            val error = location.e

            error shouldBe a[LocationNotFound[_]]
            error.getMessage should startWith("Location not available!")
          }
      }
    }
  }

  it("fails a bag if the file manifest does not exist") {
    def noDataManifest(files: StringTuple): Option[FileEntry] = None

    withLocalS3Bucket { bucket =>
      withS3Bag(bucket, createDataManifest = noDataManifest) {
        case (root, _) =>
          withVerifier { verifier =>
            val ingestStep = verifier.verify(root, externalIdentifier = createExternalIdentifier)
            val result = ingestStep.success.get

            result shouldBe a[IngestFailed[_]]
            result.summary shouldBe a[VerificationIncompleteSummary]

            val summary = result.summary
              .asInstanceOf[VerificationIncompleteSummary]
            val error = summary.e

            error shouldBe a[BagUnavailable]
            error.getMessage should include("Error loading manifest-sha256.txt")
          }
      }
    }
  }

  it("fails a bag if the tag manifest does not exist") {
    def noTagManifest(files: StringTuple): Option[FileEntry] = None

    withLocalS3Bucket { bucket =>
      withS3Bag(bucket, createTagManifest = noTagManifest) {
        case (root, _) =>
          withVerifier { verifier =>
            val ingestStep = verifier.verify(root, externalIdentifier = createExternalIdentifier)
            val result = ingestStep.success.get

            result shouldBe a[IngestFailed[_]]
            result.summary shouldBe a[VerificationIncompleteSummary]

            val summary = result.summary
              .asInstanceOf[VerificationIncompleteSummary]
            val error = summary.e

            error shouldBe a[BagUnavailable]
            error.getMessage should include(
              "Error loading tagmanifest-sha256.txt")
          }
      }
    }
  }

  it("fails if the external identifier in the bag-info.txt is incorrect") {
    val externalIdentifier = randomAlphanumeric
    val bagInfoExternalIdentifier = ExternalIdentifier(externalIdentifier + "_bag-info")
    val payloadExternalIdentifier = ExternalIdentifier(externalIdentifier + "_payload")

    withLocalS3Bucket { bucket =>
      withS3Bag(bucket, externalIdentifier = bagInfoExternalIdentifier) {
        case (root, _) =>
          withVerifier { verifier =>
            val ingestStep = verifier.verify(
              root,
              externalIdentifier = payloadExternalIdentifier
            )
            val result = ingestStep.success.get

            result shouldBe a[IngestFailed[_]]
            result.summary shouldBe a[VerificationFailureSummary]

            val userFacingMessage = result.asInstanceOf[IngestFailed[_]].maybeUserFacingMessage
            userFacingMessage.get should startWith(
              "External identifier in bag-info.txt does not match request")
          }
      }
    }
  }
}

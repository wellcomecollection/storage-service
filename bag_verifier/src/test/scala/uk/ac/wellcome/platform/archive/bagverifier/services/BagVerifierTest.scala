package uk.ac.wellcome.platform.archive.bagverifier.services

import org.scalatest.concurrent.ScalaFutures
import org.scalatest._
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
import uk.ac.wellcome.platform.archive.common.verify.{
  FailedChecksumNoMatch,
  VerifiedLocation
}

class BagVerifierTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with TryValues
    with OptionValues
    with S3BagLocationFixtures
    with BagVerifierFixtures {

  type StringTuple = List[(String, String)]

  val dataFileCount = randomInt(from = 1, to = 10)

  val expectedFileCount: Int = dataFileCount + List(
    "manifest-sha256.txt",
    "bagit.txt",
    "bag-info.txt").size

  private def verifyResultsSize(locations: Seq[VerifiedLocation], expectedSize: Int): Assertion =
    if (locations.exists { _.verifiableLocation.path.value.endsWith("fetch.txt") }) {
      locations.size shouldBe expectedSize + 1
    } else {
      locations.size shouldBe expectedSize
    }

  it("passes a bag with correct checksum values") {
    withLocalS3Bucket { bucket =>
      withS3Bag(bucket, dataFileCount = dataFileCount) {
        case (root, bagInfo) =>
          withVerifier { verifier =>
            val ingestStep =
              verifier.verify(
                root,
                externalIdentifier = bagInfo.externalIdentifier)
            val result = ingestStep.success.get

            result shouldBe a[IngestStepSucceeded[_]]
            result.summary shouldBe a[VerificationSuccessSummary]

            val summary = result.summary
              .asInstanceOf[VerificationSuccessSummary]
            val verification = summary.verification.value

            verifyResultsSize(verification.locations, expectedSize = expectedFileCount)
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
        case (root, bagInfo) =>
          withVerifier { verifier =>
            val ingestStep =
              verifier.verify(
                root,
                externalIdentifier = bagInfo.externalIdentifier)
            val result = ingestStep.success.get

            result shouldBe a[IngestFailed[_]]
            result.summary shouldBe a[VerificationFailureSummary]

            val summary = result.summary
              .asInstanceOf[VerificationFailureSummary]
            val verification = summary.verification.value

            verifyResultsSize(verification.success, expectedSize = expectedFileCount - 1)
            verification.failure should have size 1

            val location = verification.failure.head
            val error = location.e

            error shouldBe a[FailedChecksumNoMatch]
            error.getMessage should include("Checksum values do not match!")

            val userFacingMessage =
              result.asInstanceOf[IngestFailed[_]].maybeUserFacingMessage
            userFacingMessage.get shouldBe "There was 1 error verifying the bag"
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
        case (root, bagInfo) =>
          withVerifier { verifier =>
            val ingestStep =
              verifier.verify(
                root,
                externalIdentifier = bagInfo.externalIdentifier)
            val result = ingestStep.success.get

            result shouldBe a[IngestFailed[_]]
            result.summary shouldBe a[VerificationFailureSummary]

            val summary = result.summary
              .asInstanceOf[VerificationFailureSummary]
            val verification = summary.verification.value

            verifyResultsSize(verification.success, expectedSize = expectedFileCount - 1)
            verification.failure should have size 1

            val location = verification.failure.head
            val error = location.e

            error shouldBe a[FailedChecksumNoMatch]
            error.getMessage should include("Checksum values do not match!")
          }
      }
    }
  }

  it("fails a bag with multiple incorrect checksums in the file manifest") {
    withLocalS3Bucket { bucket =>
      withS3Bag(bucket, dataFileCount = 20) {
        case (root, bagInfo) =>
          // Now scribble over the contents of all the data files in the bag.
          // Note: anything referred to by the fetch file will *not* be
          // affected by this scribbling.
          val bucketKeys = listKeysInBucket(bucket)
          val badChecksumFiles =
            bucketKeys
              .filter { _.contains("/data/") }
              .map { key =>
                s3Client.putObject(
                  bucket.name,
                  key,
                  randomAlphanumeric
                )

                key
              }

          val ingestStep = withVerifier {
            _.verify(root, externalIdentifier = bagInfo.externalIdentifier)
          }

          val result = ingestStep.success.get
          result shouldBe a[IngestFailed[_]]

          val userFacingMessage =
            result.asInstanceOf[IngestFailed[_]].maybeUserFacingMessage
          userFacingMessage.get shouldBe s"There were ${badChecksumFiles.size} errors verifying the bag"
      }
    }
  }

  it("fails a bag if the file manifest refers to a non-existent file") {
    // Remove one of the valid files, replace with an invalid entry
    def createDataManifestWithExtraFile(
      dataFiles: StringTuple): Option[FileEntry] =
      createValidDataManifest(
        dataFiles.tail ++ List(("doesnotexist", "doesnotexist"))
      )

    withLocalS3Bucket { bucket =>
      withS3Bag(
        bucket,
        dataFileCount = dataFileCount,
        createDataManifest = createDataManifestWithExtraFile) {
        case (root, bagInfo) =>
          withVerifier { verifier =>
            val ingestStep =
              verifier.verify(
                root,
                externalIdentifier = bagInfo.externalIdentifier)
            val result = ingestStep.success.get

            result shouldBe a[IngestFailed[_]]
            debug(s"result = $result")
            result.summary shouldBe a[VerificationFailureSummary]

            val summary = result.summary
              .asInstanceOf[VerificationFailureSummary]
            val verification = summary.verification.value

            verifyResultsSize(verification.success, expectedSize = expectedFileCount - 1)
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
            val ingestStep =
              verifier.verify(
                root,
                externalIdentifier = createExternalIdentifier)
            val result = ingestStep.success.get

            result shouldBe a[IngestFailed[_]]
            result.summary shouldBe a[VerificationIncompleteSummary]

            val summary = result.summary
              .asInstanceOf[VerificationIncompleteSummary]
            val error = summary.e

            error shouldBe a[BagUnavailable]
            error.getMessage should include("Error loading manifest-sha256.txt")

            val userFacingMessage =
              result.asInstanceOf[IngestFailed[_]].maybeUserFacingMessage
            userFacingMessage.get shouldBe "Error loading manifest-sha256.txt: no such file!"
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
            val ingestStep =
              verifier.verify(
                root,
                externalIdentifier = createExternalIdentifier)
            val result = ingestStep.success.get

            result shouldBe a[IngestFailed[_]]
            result.summary shouldBe a[VerificationIncompleteSummary]

            val summary = result.summary
              .asInstanceOf[VerificationIncompleteSummary]
            val error = summary.e

            error shouldBe a[BagUnavailable]
            error.getMessage should include(
              "Error loading tagmanifest-sha256.txt")

            val userFacingMessage =
              result.asInstanceOf[IngestFailed[_]].maybeUserFacingMessage
            userFacingMessage.get shouldBe "Error loading tagmanifest-sha256.txt: no such file!"
          }
      }
    }
  }

  it("fails if the external identifier in the bag-info.txt is incorrect") {
    val externalIdentifier = randomAlphanumeric
    val bagInfoExternalIdentifier =
      ExternalIdentifier(externalIdentifier + "_bag-info")
    val payloadExternalIdentifier =
      ExternalIdentifier(externalIdentifier + "_payload")

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
            result.summary shouldBe a[VerificationIncompleteSummary]

            val userFacingMessage =
              result.asInstanceOf[IngestFailed[_]].maybeUserFacingMessage
            userFacingMessage.get should startWith(
              "External identifier in bag-info.txt does not match request")
          }
      }
    }
  }

  describe("checks for unreferenced files") {
    it("fails if there is one unreferenced file") {
      withLocalS3Bucket { bucket =>
        withS3Bag(bucket) {
          case (root, bagInfo) =>
            val location = root.join("unreferencedfile.txt")
            s3Client.putObject(
              location.namespace,
              location.path,
              randomAlphanumeric
            )

            withVerifier { verifier =>
              val ingestStep = verifier.verify(
                root,
                externalIdentifier = bagInfo.externalIdentifier
              )
              val result = ingestStep.success.get

              result shouldBe a[IngestFailed[_]]
              val ingestFailed = result.asInstanceOf[IngestFailed[_]]

              ingestFailed.e.getMessage shouldBe
                s"Bag contains a file which is not referenced in the manifest: $location"

              ingestFailed.maybeUserFacingMessage.get shouldBe
                "Bag contains a file which is not referenced in the manifest: /unreferencedfile.txt"
            }
        }
      }
    }

    it("fails if there are multiple unreferenced files") {
      withLocalS3Bucket { bucket =>
        withS3Bag(bucket) {
          case (root, bagInfo) =>
            val locations = (1 to 3).map { i =>
              val location = root.join(s"unreferencedfile_$i.txt")
              s3Client.putObject(
                location.namespace,
                location.path,
                randomAlphanumeric
              )
              location
            }

            withVerifier { verifier =>
              val ingestStep = verifier.verify(
                root,
                externalIdentifier = bagInfo.externalIdentifier
              )
              val result = ingestStep.success.get

              result shouldBe a[IngestFailed[_]]
              val ingestFailed = result.asInstanceOf[IngestFailed[_]]

              ingestFailed.e.getMessage shouldBe
                s"Bag contains 3 files which are not referenced in the manifest: ${locations.mkString(", ")}"

              ingestFailed.maybeUserFacingMessage.get shouldBe
                s"Bag contains 3 files which are not referenced in the manifest: " +
                  "/unreferencedfile_1.txt, /unreferencedfile_2.txt, /unreferencedfile_3.txt"
            }
        }
      }
    }

  }

  describe("checks the Payload-Oxum") {
    it("fails if the Payload-Oxum has the wrong file count") {
      withLocalS3Bucket { bucket =>
        withS3Bag(
          bucket,
          payloadOxum = Some(
            createPayloadOxumWith(
              numberOfPayloadFiles = dataFileCount - 1
            )
          ),
          dataFileCount = dataFileCount) {
          case (root, bagInfo) =>
            withVerifier { verifier =>
              val ingestStep = verifier.verify(
                root,
                externalIdentifier = bagInfo.externalIdentifier
              )
              val result = ingestStep.success.get

              result shouldBe a[IngestFailed[_]]
              result.summary shouldBe a[VerificationIncompleteSummary]

              val userFacingMessage =
                result.asInstanceOf[IngestFailed[_]].maybeUserFacingMessage
              userFacingMessage.get shouldBe
                s"""Payload-Oxum has the wrong number of payload files: ${dataFileCount - 1}, but bag manifest has $dataFileCount"""
            }
        }
      }
    }

    it("fails if the Payload-Oxum has the wrong octet count") {
      withLocalS3Bucket { bucket =>
        withS3Bag(
          bucket,
          payloadOxum = Some(
            createPayloadOxumWith(
              payloadBytes = 0,
              numberOfPayloadFiles = dataFileCount
            )
          ),
          dataFileCount = dataFileCount) {
          case (root, bagInfo) =>
            withVerifier { verifier =>
              val ingestStep = verifier.verify(
                root,
                externalIdentifier = bagInfo.externalIdentifier
              )
              val result = ingestStep.success.get

              result shouldBe a[IngestFailed[_]]
              result.summary shouldBe a[VerificationIncompleteSummary]

              val userFacingMessage =
                result.asInstanceOf[IngestFailed[_]].maybeUserFacingMessage
              userFacingMessage.get should fullyMatch regex
                s"""Payload-Oxum has the wrong octetstream sum: 0 bytes, but bag actually contains \\d+ bytes"""
            }
        }
      }
    }
  }
}

package uk.ac.wellcome.platform.archive.bagverifier.services

import org.scalatest.concurrent.ScalaFutures
import org.scalatest._
import uk.ac.wellcome.platform.archive.bagverifier.fixtures.BagVerifierFixtures
import uk.ac.wellcome.platform.archive.bagverifier.models.{
  VerificationFailureSummary,
  VerificationIncompleteSummary,
  VerificationSuccessSummary
}
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagPath,
  ExternalIdentifier,
  PayloadOxum
}
import uk.ac.wellcome.platform.archive.common.bagit.services.BagUnavailable
import uk.ac.wellcome.platform.archive.common.bagit.services.s3.S3BagReader
import uk.ac.wellcome.platform.archive.common.fixtures.{
  S3BagBuilder,
  S3BagBuilderBase
}
import uk.ac.wellcome.platform.archive.common.storage.LocationNotFound
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestFailed,
  IngestStepSucceeded
}
import uk.ac.wellcome.platform.archive.common.verify.{
  FailedChecksumNoMatch,
  VerifiedSuccess
}

class BagVerifierTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with TryValues
    with OptionValues
    with BagVerifierFixtures {

  type StringTuple = List[(String, String)]

  val payloadFileCount: Int = randomInt(from = 1, to = 10)

  val expectedFileCount: Int = payloadFileCount + List(
    "manifest-sha256.txt",
    "bagit.txt",
    "bag-info.txt").size

  it("passes a bag with correct checksum values") {
    withLocalS3Bucket { bucket =>
      val (root, bagInfo) = S3BagBuilder.createS3BagWith(
        bucket,
        payloadFileCount = payloadFileCount)

      println(root)
      println(listKeysInBucket(bucket))

      val ingestStep =
        withVerifier {
          _.verify(root, externalIdentifier = bagInfo.externalIdentifier)
        }

      val result = ingestStep.success.get

      result shouldBe a[IngestStepSucceeded[_]]
      result.summary shouldBe a[VerificationSuccessSummary]

      val summary = result.summary
        .asInstanceOf[VerificationSuccessSummary]
      val verification = summary.verification.value

      verifySuccessCount(
        verification.locations,
        expectedCount = expectedFileCount)
    }
  }

  it("fails a bag with an incorrect checksum in the file manifest") {
    withLocalS3Bucket { bucket =>
      val badBuilder = new S3BagBuilderBase {
        override protected def createPayloadManifest(
          entries: Seq[PayloadEntry]): Option[String] =
          super.createPayloadManifest(
            entries.head.copy(contents = randomAlphanumeric) +: entries.tail
          )
      }

      val (root, bagInfo) =
        badBuilder.createS3BagWith(bucket, payloadFileCount = payloadFileCount)

      val ingestStep =
        withVerifier {
          _.verify(root, externalIdentifier = bagInfo.externalIdentifier)
        }

      val result = ingestStep.success.get

      result shouldBe a[IngestFailed[_]]
      result.summary shouldBe a[VerificationFailureSummary]

      val summary = result.summary
        .asInstanceOf[VerificationFailureSummary]
      val verification = summary.verification.value

      verifySuccessCount(
        verification.success,
        expectedCount = expectedFileCount - 1)
      verification.failure should have size 1

      val location = verification.failure.head
      val error = location.e

      error shouldBe a[FailedChecksumNoMatch]
      error.getMessage should include("Checksum values do not match!")

      val userFacingMessage =
        result.asInstanceOf[IngestFailed[_]].maybeUserFacingMessage
      userFacingMessage.get should startWith(
        "Unable to verify one file in the bag:")
    }
  }

  it("fails a bag with an incorrect checksum in the tag manifest") {
    withLocalS3Bucket { bucket =>
      val badBuilder = new S3BagBuilderBase {
        override protected def createTagManifest(
          entries: Seq[ManifestFile]): Option[String] =
          super.createTagManifest(
            entries.head.copy(contents = randomAlphanumeric) +: entries.tail
          )
      }

      val (root, bagInfo) =
        badBuilder.createS3BagWith(bucket, payloadFileCount = payloadFileCount)

      val ingestStep =
        withVerifier {
          _.verify(root, externalIdentifier = bagInfo.externalIdentifier)
        }

      val result = ingestStep.success.get

      result shouldBe a[IngestFailed[_]]
      result.summary shouldBe a[VerificationFailureSummary]

      val summary = result.summary
        .asInstanceOf[VerificationFailureSummary]
      val verification = summary.verification.value

      verifySuccessCount(
        verification.success,
        expectedCount = expectedFileCount - 1)
      verification.failure should have size 1

      val location = verification.failure.head
      val error = location.e

      error shouldBe a[FailedChecksumNoMatch]
      error.getMessage should include("Checksum values do not match!")
    }
  }

  it("fails a bag with multiple incorrect checksums in the file manifest") {
    withLocalS3Bucket { bucket =>
      val badBuilder = new S3BagBuilderBase {
        override protected def createPayloadManifest(
          entries: Seq[PayloadEntry]): Option[String] =
          super.createPayloadManifest(
            entries.map { _.copy(contents = randomAlphanumeric) }
          )
      }

      val (root, bagInfo) =
        badBuilder.createS3BagWith(bucket, payloadFileCount = payloadFileCount)

      val ingestStep =
        withVerifier {
          _.verify(root, externalIdentifier = bagInfo.externalIdentifier)
        }

      val result = ingestStep.success.get
      result shouldBe a[IngestFailed[_]]

      val userFacingMessage =
        result.asInstanceOf[IngestFailed[_]].maybeUserFacingMessage
      userFacingMessage.get should startWith(
        s"Unable to verify $payloadFileCount files in the bag:")
    }
  }

  it("fails a bag if the file manifest refers to a non-existent file") {
    withLocalS3Bucket { bucket =>
      val badBuilder = new S3BagBuilderBase {
        override protected def createPayloadManifest(
          entries: Seq[PayloadEntry]): Option[String] =
          super.createPayloadManifest(
            entries.tail :+ PayloadEntry(
              bagPath = BagPath(randomAlphanumeric),
              path = randomAlphanumeric,
              contents = randomAlphanumeric)
          )

        // This ensures that the fetch file won't refer to the entry
        // we've deleted from the manifest.
        override protected def getFetchEntryCount(payloadFileCount: Int) = 0
      }

      val (root, bagInfo) =
        badBuilder.createS3BagWith(bucket, payloadFileCount = payloadFileCount)

      val ingestStep =
        withVerifier {
          _.verify(root, externalIdentifier = bagInfo.externalIdentifier)
        }

      val result = ingestStep.success.get

      result shouldBe a[IngestFailed[_]]
      debug(s"result = $result")
      result.summary shouldBe a[VerificationFailureSummary]

      val summary = result.summary
        .asInstanceOf[VerificationFailureSummary]
      val verification = summary.verification.value

      verifySuccessCount(
        verification.success,
        expectedCount = expectedFileCount - 1)
      verification.failure should have size 1

      val location = verification.failure.head
      val error = location.e

      error shouldBe a[LocationNotFound[_]]
      error.getMessage should startWith("Location not available!")
    }
  }

  it("fails a bag if the file manifest does not exist") {
    withLocalS3Bucket { bucket =>
      val badBuilder = new S3BagBuilderBase {
        override protected def createPayloadManifest(
          entries: Seq[PayloadEntry]): Option[String] =
          None
      }

      val (root, bagInfo) = badBuilder.createS3BagWith(bucket)

      val ingestStep =
        withVerifier {
          _.verify(root, externalIdentifier = bagInfo.externalIdentifier)
        }

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

  it("fails a bag if the tag manifest does not exist") {
    withLocalS3Bucket { bucket =>
      val badBuilder = new S3BagBuilderBase {
        override protected def createTagManifest(
          entries: Seq[ManifestFile]): Option[String] =
          None
      }

      val (root, bagInfo) = badBuilder.createS3BagWith(bucket)

      val ingestStep =
        withVerifier {
          _.verify(root, externalIdentifier = bagInfo.externalIdentifier)
        }

      val result = ingestStep.success.get

      result shouldBe a[IngestFailed[_]]
      result.summary shouldBe a[VerificationIncompleteSummary]

      val summary = result.summary
        .asInstanceOf[VerificationIncompleteSummary]
      val error = summary.e

      error shouldBe a[BagUnavailable]
      error.getMessage should include("Error loading tagmanifest-sha256.txt")

      val userFacingMessage =
        result.asInstanceOf[IngestFailed[_]].maybeUserFacingMessage
      userFacingMessage.get shouldBe "Error loading tagmanifest-sha256.txt: no such file!"
    }
  }

  it("fails if the external identifier in the bag-info.txt is incorrect") {
    val externalIdentifier = randomAlphanumeric
    val bagInfoExternalIdentifier =
      ExternalIdentifier(externalIdentifier + "_bag-info")
    val payloadExternalIdentifier =
      ExternalIdentifier(externalIdentifier + "_payload")

    withLocalS3Bucket { bucket =>
      val (root, _) = S3BagBuilder.createS3BagWith(
        bucket,
        externalIdentifier = bagInfoExternalIdentifier)

      val ingestStep =
        withVerifier {
          _.verify(root, externalIdentifier = payloadExternalIdentifier)
        }

      val result = ingestStep.success.get

      result shouldBe a[IngestFailed[_]]
      result.summary shouldBe a[VerificationIncompleteSummary]

      val userFacingMessage =
        result.asInstanceOf[IngestFailed[_]].maybeUserFacingMessage
      userFacingMessage.get should startWith(
        "External identifier in bag-info.txt does not match request")
    }
  }

  describe("checks the fetch file") {
    it("fails if the fetch file refers to a file not in the manifest") {
      withLocalS3Bucket { bucket =>
        val badBuilder = new S3BagBuilderBase {
          override protected def createFetchFile(entries: Seq[PayloadEntry])(
            implicit namespace: String): Option[String] =
            super.createFetchFile(
              entries :+
                PayloadEntry(
                  bagPath = BagPath("data/doesnotexist"),
                  path = "data/doesnotexist",
                  contents = randomAlphanumeric
                ))
        }

        val (root, bagInfo) =
          badBuilder.createS3BagWith(bucket)

        val ingestStep =
          withVerifier {
            _.verify(root, externalIdentifier = bagInfo.externalIdentifier)
          }

        val result = ingestStep.success.get

        result shouldBe a[IngestFailed[_]]
        debug(s"result = $result")
        result.summary shouldBe a[VerificationIncompleteSummary]

        val ingestFailed = result.asInstanceOf[IngestFailed[_]]

        ingestFailed.e.getMessage shouldBe
          "Fetch entry refers to a path that isn't in the bag manifest: data/doesnotexist"

        ingestFailed.maybeUserFacingMessage.get shouldBe
          "Fetch entry refers to a path that isn't in the bag manifest: data/doesnotexist"
      }
    }
  }

  describe("checks for unreferenced files") {
    it("fails if there is one unreferenced file") {
      withLocalS3Bucket { bucket =>
        val (root, bagInfo) = S3BagBuilder.createS3BagWith(bucket)

        val location = root.join("unreferencedfile.txt")
        s3Client.putObject(
          location.namespace,
          location.path,
          randomAlphanumeric
        )

        val ingestStep =
          withVerifier {
            _.verify(root, externalIdentifier = bagInfo.externalIdentifier)
          }

        val result = ingestStep.success.get

        result shouldBe a[IngestFailed[_]]
        val ingestFailed = result.asInstanceOf[IngestFailed[_]]

        ingestFailed.e.getMessage shouldBe
          s"Bag contains a file which is not referenced in the manifest: $location"

        ingestFailed.maybeUserFacingMessage.get shouldBe
          "Bag contains a file which is not referenced in the manifest: /unreferencedfile.txt"
      }
    }

    it("fails if there are multiple unreferenced files") {
      withLocalS3Bucket { bucket =>
        val (root, bagInfo) = S3BagBuilder.createS3BagWith(bucket)

        val locations = (1 to 3).map { i =>
          val location = root.join(s"unreferencedfile_$i.txt")
          s3Client.putObject(
            location.namespace,
            location.path,
            randomAlphanumeric
          )
          location
        }

        val ingestStep =
          withVerifier {
            _.verify(root, externalIdentifier = bagInfo.externalIdentifier)
          }

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

    it("fails if a file in the fetch.txt also appears in the bag") {
      withLocalS3Bucket { bucket =>
        val alwaysWriteAsFetchBuilder = new S3BagBuilderBase {
          override protected def getFetchEntryCount(
            payloadFileCount: Int): Int =
            payloadFileCount
        }

        val (root, bagInfo) = alwaysWriteAsFetchBuilder.createS3BagWith(bucket)

        val bag = new S3BagReader().get(root).right.value

        // Write one of the fetch.txt entries as a concrete file
        val badFetchEntry = bag.fetch.get.files.head
        val badFetchLocation = root.join(badFetchEntry.path.value)

        s3Client.putObject(
          badFetchLocation.namespace,
          badFetchLocation.path,
          randomAlphanumeric
        )

        val ingestStep =
          withVerifier {
            _.verify(root, externalIdentifier = bagInfo.externalIdentifier)
          }

        val result = ingestStep.success.get

        result shouldBe a[IngestFailed[_]]
        val ingestFailed = result.asInstanceOf[IngestFailed[_]]

        ingestFailed.e.getMessage shouldBe
          s"Files referred to in the fetch.txt also appear in the bag: ${root
            .join(badFetchEntry.path.value)}"

        ingestFailed.maybeUserFacingMessage.get shouldBe
          s"Files referred to in the fetch.txt also appear in the bag: ${badFetchEntry.path}"
      }
    }

    it("passes a bag that includes a manifest/tag manifest for another algorithm") {
      withLocalS3Bucket { bucket =>
        val (root, bagInfo) = S3BagBuilder.createS3BagWith(bucket)

        val location = root.join("tagmanifest-sha512.txt")

        s3Client.putObject(
          location.namespace,
          location.path,
          "<empty SHA512 tag manifest>"
        )

        val ingestStep =
          withVerifier {
            _.verify(root, externalIdentifier = bagInfo.externalIdentifier)
          }

        ingestStep.success.get shouldBe a[IngestStepSucceeded[_]]
      }
    }
  }

  describe("checks the Payload-Oxum") {
    it("fails if the Payload-Oxum has the wrong file count") {
      withLocalS3Bucket { bucket =>
        val badBuilder = new S3BagBuilderBase {
          override protected def createPayloadOxum(
            entries: Seq[PayloadEntry]): PayloadOxum = {
            val oxum = super.createPayloadOxum(entries)

            oxum.copy(numberOfPayloadFiles = oxum.numberOfPayloadFiles - 1)
          }
        }

        val (root, bagInfo) = badBuilder.createS3BagWith(
          bucket,
          payloadFileCount = payloadFileCount)

        val ingestStep =
          withVerifier {
            _.verify(root, externalIdentifier = bagInfo.externalIdentifier)
          }

        val result = ingestStep.success.get

        result shouldBe a[IngestFailed[_]]
        result.summary shouldBe a[VerificationIncompleteSummary]

        val userFacingMessage =
          result.asInstanceOf[IngestFailed[_]].maybeUserFacingMessage
        userFacingMessage.get shouldBe
          s"""Payload-Oxum has the wrong number of payload files: ${payloadFileCount - 1}, but bag manifest has $payloadFileCount"""
      }
    }

    it("fails if the Payload-Oxum has the wrong octet count") {
      withLocalS3Bucket { bucket =>
        val badBuilder = new S3BagBuilderBase {
          override protected def createPayloadOxum(
            entries: Seq[PayloadEntry]): PayloadOxum = {
            val oxum = super.createPayloadOxum(entries)

            oxum.copy(payloadBytes = oxum.payloadBytes - 1)
          }
        }

        val (root, bagInfo) = badBuilder.createS3BagWith(bucket)

        val ingestStep =
          withVerifier {
            _.verify(root, externalIdentifier = bagInfo.externalIdentifier)
          }

        val result = ingestStep.success.get

        result shouldBe a[IngestFailed[_]]
        result.summary shouldBe a[VerificationIncompleteSummary]

        val userFacingMessage =
          result.asInstanceOf[IngestFailed[_]].maybeUserFacingMessage
        userFacingMessage.get should fullyMatch regex
          s"""Payload-Oxum has the wrong octetstream sum: \\d+ bytes, but bag actually contains \\d+ bytes"""
      }
    }
  }

  private def verifySuccessCount(successes: List[VerifiedSuccess],
                                 expectedCount: Int): Assertion =
    if (successes.map { _.objectLocation.path }.exists {
          _.endsWith("/fetch.txt")
        }) {
      successes.size shouldBe expectedCount + 1
    } else {
      successes.size shouldBe expectedCount
    }
}

package uk.ac.wellcome.platform.archive.bagverifier.services

import org.scalatest.{Assertion, OptionValues, TryValues}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.bagverifier.fixtures.BagVerifierFixtures
import uk.ac.wellcome.platform.archive.bagverifier.models.{
  VerificationFailureSummary,
  VerificationIncompleteSummary,
  VerificationSuccessSummary,
  VerificationSummary
}
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagInfo,
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
  IngestStepResult,
  IngestStepSucceeded
}
import uk.ac.wellcome.platform.archive.common.verify.{
  FailedChecksumNoMatch,
  VerifiedSuccess
}
import uk.ac.wellcome.storage.ObjectLocationPrefix
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket

class BagVerifierTest
    extends AnyFunSpec
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
    "bag-info.txt"
  ).size

  it("passes a bag with correct checksum values") {
    withLocalS3Bucket { bucket =>
      val (bagRoot, bagInfo) = S3BagBuilder.createS3BagWith(
        bucket,
        payloadFileCount = payloadFileCount
      )

      val ingestStep =
        withVerifier {
          _.verify(
            ingestId = createIngestID,
            root = bagRoot,
            externalIdentifier = bagInfo.externalIdentifier
          )
        }

      val result = ingestStep.success.get

      result shouldBe a[IngestStepSucceeded[_]]
      result.summary shouldBe a[VerificationSuccessSummary]

      val summary = result.summary
        .asInstanceOf[VerificationSuccessSummary]
      val verification = summary.verification.value

      verifySuccessCount(
        verification.locations,
        expectedCount = expectedFileCount
      )
    }
  }

  it("fails a bag with an incorrect checksum in the file manifest") {
    val badBuilder = new S3BagBuilderBase {
      override protected def createPayloadManifest(entries: Seq[PayloadEntry]): Option[String] =
        super.createPayloadManifest(
          entries.head.copy(contents = randomAlphanumeric) +: entries.tail
        )
    }

    assertBagFails(badBuilder) { case (ingestFailed, summary) =>
      val verification = summary.verification.value

      verifySuccessCount(
        verification.success,
        expectedCount = expectedFileCount - 1
      )
      verification.failure should have size 1

      val location = verification.failure.head
      val error = location.e

      error shouldBe a[FailedChecksumNoMatch]
      error.getMessage should include("Checksum values do not match!")

      ingestFailed.maybeUserFacingMessage.get should startWith(
        "Unable to verify one file in the bag:"
      )
    }
  }

  it("fails a bag with an incorrect checksum in the tag manifest") {
    val badBuilder = new S3BagBuilderBase {
      override protected def createTagManifest(entries: Seq[ManifestFile]): Option[String] =
        super.createTagManifest(
          entries.head.copy(contents = randomAlphanumeric) +: entries.tail
        )
    }

    assertBagFails(badBuilder) { case (_, summary) =>
      val verification = summary.verification.value

      verifySuccessCount(
        verification.success,
        expectedCount = expectedFileCount - 1
      )
      verification.failure should have size 1

      val location = verification.failure.head
      val error = location.e

      error shouldBe a[FailedChecksumNoMatch]
      error.getMessage should include("Checksum values do not match!")
    }
  }

  it("fails a bag with multiple incorrect checksums in the file manifest") {
    val badBuilder = new S3BagBuilderBase {
      override protected def createPayloadManifest(entries: Seq[PayloadEntry]): Option[String] =
        super.createPayloadManifest(
          entries.map { _.copy(contents = randomAlphanumeric) }
        )
    }

    assertBagFails(badBuilder) { case (ingestFailed, _) =>
      ingestFailed.maybeUserFacingMessage.get should startWith(
        s"Unable to verify $payloadFileCount files in the bag:"
      )
    }
  }

  it("fails a bag if the file manifest refers to a non-existent file") {
    val badBuilder = new S3BagBuilderBase {
      override protected def createPayloadManifest(
                                                    entries: Seq[PayloadEntry]
                                                  ): Option[String] =
        super.createPayloadManifest(
          entries.tail :+ PayloadEntry(
            bagPath = BagPath(randomAlphanumeric),
            path = randomAlphanumeric,
            contents = randomAlphanumeric
          )
        )

      // This ensures that the fetch file won't refer to the entry
      // we've deleted from the manifest.
      override protected def getFetchEntryCount(payloadFileCount: Int): Int = 0
    }

    assertBagFails(badBuilder) { case (_, summary) =>
      val verification = summary.verification.value

      verifySuccessCount(
        verification.success,
        expectedCount = expectedFileCount - 1
      )
      verification.failure should have size 1

      val location = verification.failure.head
      val error = location.e

      error shouldBe a[LocationNotFound[_]]
      error.getMessage should startWith("Location not available!")
    }
  }

  it("fails a bag if the file manifest does not exist") {
    val badBuilder = new S3BagBuilderBase {
      override protected def createPayloadManifest(entries: Seq[PayloadEntry]): Option[String] =
        None
    }

    assertBagIncomplete(badBuilder) { case (ingestFailed, summary) =>
      val error = summary.e

      error shouldBe a[BagUnavailable]
      error.getMessage should include("Error loading manifest-sha256.txt")

      ingestFailed.maybeUserFacingMessage.get shouldBe "Error loading manifest-sha256.txt: no such file!"
    }
  }

  it("fails a bag if the tag manifest does not exist") {
    val badBuilder = new S3BagBuilderBase {
      override protected def createTagManifest(entries: Seq[ManifestFile]): Option[String] =
        None
    }

    assertBagIncomplete(badBuilder) { case (ingestFailed, summary) =>
      val error = summary.e

      error shouldBe a[BagUnavailable]
      error.getMessage should include("Error loading tagmanifest-sha256.txt")

      ingestFailed.maybeUserFacingMessage.get shouldBe "Error loading tagmanifest-sha256.txt: no such file!"
    }
  }

  it("fails if the external identifier in the bag-info.txt is incorrect") {
    val externalIdentifier = randomAlphanumeric
    val bagInfoExternalIdentifier =
      ExternalIdentifier(externalIdentifier + "_bag-info")
    val payloadExternalIdentifier =
      ExternalIdentifier(externalIdentifier + "_payload")

    withLocalS3Bucket { bucket =>
      val (bagRoot, _) = S3BagBuilder.createS3BagWith(
        bucket,
        externalIdentifier = bagInfoExternalIdentifier
      )

      val ingestStep =
        withVerifier {
          _.verify(
            ingestId = createIngestID,
            root = bagRoot,
            externalIdentifier = payloadExternalIdentifier
          )
        }

      val result = ingestStep.success.get

      result shouldBe a[IngestFailed[_]]
      result.summary shouldBe a[VerificationIncompleteSummary]

      result.maybeUserFacingMessage.get should startWith(
        "External identifier in bag-info.txt does not match request"
      )
    }
  }

  describe("checks the fetch file") {
    it("fails if the fetch file refers to a file not in the manifest") {
      val badBuilder = new S3BagBuilderBase {
        override protected def createFetchFile(
          entries: Seq[PayloadEntry]
        )(implicit namespace: String): Option[String] =
          super.createFetchFile(
            entries :+
              PayloadEntry(
                bagPath = BagPath("data/doesnotexist"),
                path = "data/doesnotexist",
                contents = randomAlphanumeric
              )
          )
      }

      assertBagIncomplete(badBuilder) { case (ingestFailed, _) =>
        ingestFailed.e.getMessage shouldBe
          "fetch.txt refers to paths that aren't in the bag manifest: data/doesnotexist"

        ingestFailed.maybeUserFacingMessage.get shouldBe
          "fetch.txt refers to paths that aren't in the bag manifest: data/doesnotexist"
      }
    }
  }

  describe("checks for unreferenced files") {
    it("fails if there is one unreferenced file") {
      val badBuilder = new S3BagBuilderBase {
        override def createS3BagWith(
          bucket: Bucket,
          externalIdentifier: ExternalIdentifier = createExternalIdentifier,
          payloadFileCount: Int = randomInt(from = 5, to = 50)
        ): (ObjectLocationPrefix, BagInfo) = {
          val (bagRoot, bagInfo) =
            super.createS3BagWith(bucket, externalIdentifier, payloadFileCount)

          val location = bagRoot.asLocation("unreferencedfile.txt")
          s3Client.putObject(
            location.namespace,
            location.path,
            randomAlphanumeric
          )

          (bagRoot, bagInfo)
        }
      }

      assertBagIncomplete(badBuilder) { case (ingestFailed, _) =>
        ingestFailed.e.getMessage should startWith("Bag contains a file which is not referenced in the manifest:")

        ingestFailed.maybeUserFacingMessage.get shouldBe
          "Bag contains a file which is not referenced in the manifest: /unreferencedfile.txt"
      }
    }

    it("fails if there are multiple unreferenced files") {
      val badBuilder = new S3BagBuilderBase {
        override def createS3BagWith(
          bucket: Bucket,
          externalIdentifier: ExternalIdentifier = createExternalIdentifier,
          payloadFileCount: Int = randomInt(from = 5, to = 50)
        ): (ObjectLocationPrefix, BagInfo) = {
          val (bagRoot, bagInfo) =
            super.createS3BagWith(bucket, externalIdentifier, payloadFileCount)

          (1 to 3).foreach { i =>
            val location = bagRoot.asLocation(s"unreferencedfile_$i.txt")

            s3Client.putObject(
              location.namespace,
              location.path,
              randomAlphanumeric
            )
          }

          (bagRoot, bagInfo)
        }
      }

      assertBagIncomplete(badBuilder) { case (ingestFailed, _) =>
        ingestFailed.e.getMessage should startWith("Bag contains 3 files which are not referenced in the manifest:")

        ingestFailed.maybeUserFacingMessage.get shouldBe
          s"Bag contains 3 files which are not referenced in the manifest: " +
            "/unreferencedfile_1.txt, /unreferencedfile_2.txt, /unreferencedfile_3.txt"
      }
    }

    it("fails if a file in the fetch.txt also appears in the bag") {
      val alwaysWriteAsFetchBuilder = new S3BagBuilderBase {
        override protected def getFetchEntryCount(payloadFileCount: Int): Int =
          payloadFileCount

        override def createS3BagWith(
          bucket: Bucket,
          externalIdentifier: ExternalIdentifier = createExternalIdentifier,
          payloadFileCount: Int = randomInt(from = 5, to = 50)
        ): (ObjectLocationPrefix, BagInfo) = {
          val (bagRoot, bagInfo) =
            super.createS3BagWith(bucket, externalIdentifier, payloadFileCount)

          val bag = new S3BagReader().get(bagRoot).right.value

          // Write one of the fetch.txt entries as a concrete file
          val badFetchPath: BagPath = bag.fetch.get.paths.head
          val badFetchLocation = bagRoot.asLocation(badFetchPath.value)

          s3Client.putObject(
            badFetchLocation.namespace,
            badFetchLocation.path,
            randomAlphanumeric
          )

          (bagRoot, bagInfo)
        }
      }

      assertBagIncomplete(alwaysWriteAsFetchBuilder) { case (ingestFailed, _) =>
        ingestFailed.e.getMessage should startWith("Files referred to in the fetch.txt also appear in the bag:")

        ingestFailed.maybeUserFacingMessage.get should startWith("Files referred to in the fetch.txt also appear in the bag:")
      }
    }

    it("passes a bag that includes an extra manifest/tag manifest") {
      withLocalS3Bucket { bucket =>
        val (bagRoot, bagInfo) = S3BagBuilder.createS3BagWith(bucket)

        val location = bagRoot.asLocation("tagmanifest-sha512.txt")

        s3Client.putObject(
          location.namespace,
          location.path,
          "<empty SHA512 tag manifest>"
        )

        val ingestStep =
          withVerifier {
            _.verify(
              ingestId = createIngestID,
              root = bagRoot,
              externalIdentifier = bagInfo.externalIdentifier
            )
          }

        ingestStep.success.get shouldBe a[IngestStepSucceeded[_]]
      }
    }
  }

  describe("checks the Payload-Oxum") {
    it("fails if the Payload-Oxum has the wrong file count") {
      val badBuilder = new S3BagBuilderBase {
        override protected def createPayloadOxum(entries: Seq[PayloadEntry]): PayloadOxum = {
          val oxum = super.createPayloadOxum(entries)

          oxum.copy(numberOfPayloadFiles = oxum.numberOfPayloadFiles - 1)
        }
      }

      assertBagIncomplete(badBuilder) { case (ingestFailed, _) =>
        ingestFailed.maybeUserFacingMessage.get shouldBe
          s"""Payload-Oxum has the wrong number of payload files: ${payloadFileCount - 1}, but bag manifest has $payloadFileCount"""
      }
    }

    it("fails if the Payload-Oxum has the wrong octet count") {
      val badBuilder = new S3BagBuilderBase {
        override protected def createPayloadOxum(entries: Seq[PayloadEntry]): PayloadOxum = {
          val oxum = super.createPayloadOxum(entries)

          oxum.copy(payloadBytes = oxum.payloadBytes - 1)
        }
      }

      assertBagIncomplete(badBuilder) { case (ingestFailed, _) =>
        ingestFailed.maybeUserFacingMessage.get should fullyMatch regex
          s"""Payload-Oxum has the wrong octetstream sum: \\d+ bytes, but bag actually contains \\d+ bytes"""
      }
    }
  }

  private def verifySuccessCount(
    successes: List[VerifiedSuccess],
    expectedCount: Int
  ): Assertion =
    if (successes.map { _.objectLocation.path }.exists {
          _.endsWith("/fetch.txt")
        }) {
      successes.size shouldBe expectedCount + 1
    } else {
      successes.size shouldBe expectedCount
    }

  // Given a builder that fails to create a valid bag for some reason, ensure that
  // it is caught correctly by the verifier.
  private def assertBagResultFails(
    badBuilder: S3BagBuilderBase)(
    assertion: IngestStepResult[VerificationSummary] => Assertion): Assertion =
    withLocalS3Bucket { bucket =>
      val (bagRoot, bagInfo) = badBuilder.createS3BagWith(
        bucket,
        payloadFileCount = payloadFileCount
      )

      val ingestStep =
        withVerifier {
          _.verify(
            ingestId = createIngestID,
            root = bagRoot,
            externalIdentifier = bagInfo.externalIdentifier
          )
        }

      val result = ingestStep.success.get
      debug(s"result = $result")

      result shouldBe a[IngestFailed[_]]
      assertion(result)
    }

  private def assertBagFails(
    badBuilder: S3BagBuilderBase)(
    assertion: (IngestFailed[VerificationFailureSummary], VerificationFailureSummary) => Assertion): Assertion =
    assertBagResultFails(badBuilder) { result =>
      result.summary shouldBe a[VerificationFailureSummary]

      val failedResult = result.asInstanceOf[IngestFailed[VerificationFailureSummary]]
      val summary = result.summary.asInstanceOf[VerificationFailureSummary]

      assertion(failedResult, summary)
    }

  private def assertBagIncomplete(
    badBuilder: S3BagBuilderBase)(
    assertion: (IngestFailed[VerificationIncompleteSummary], VerificationIncompleteSummary) => Assertion): Assertion =
    assertBagResultFails(badBuilder) { result =>
      result.summary shouldBe a[VerificationIncompleteSummary]

      val failedResult = result.asInstanceOf[IngestFailed[VerificationIncompleteSummary]]
      val summary = result.summary.asInstanceOf[VerificationIncompleteSummary]

      assertion(failedResult, summary)
    }
}

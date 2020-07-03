package uk.ac.wellcome.platform.archive.bagverifier.services

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, OptionValues, TryValues}
import uk.ac.wellcome.platform.archive.bagverifier.fixity.{
  FailedChecksumNoMatch,
  FileFixityCorrect
}
import uk.ac.wellcome.platform.archive.bagverifier.fixtures.BagVerifierFixtures
import uk.ac.wellcome.platform.archive.bagverifier.models.{
  VerificationFailureSummary,
  VerificationIncompleteSummary,
  VerificationSuccessSummary,
  VerificationSummary
}
import uk.ac.wellcome.platform.archive.bagverifier.storage.LocationNotFound
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagInfo,
  BagPath,
  ExternalIdentifier,
  PayloadOxum
}
import uk.ac.wellcome.platform.archive.common.bagit.services.BagUnavailable
import uk.ac.wellcome.platform.archive.common.bagit.services.s3.S3BagReader
import uk.ac.wellcome.platform.archive.common.fixtures.s3.S3BagBuilder
import uk.ac.wellcome.platform.archive.common.generators.StorageSpaceGenerators
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestFailed,
  IngestStepResult,
  IngestStepSucceeded,
  StorageSpace
}
import uk.ac.wellcome.storage.{S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket

class BagVerifierTest
    extends AnyFunSpec
    with Matchers
    with TryValues
    with OptionValues
    with BagVerifierFixtures
    with StorageSpaceGenerators
    with S3BagBuilder {

  val payloadFileCount: Int = randomInt(from = 1, to = 10)

  val expectedFileCount: Int = payloadFileCount + List(
    "manifest-sha256.txt",
    "bagit.txt",
    "bag-info.txt"
  ).size

  it("passes a bag with correct checksum values") {
    withLocalS3Bucket { bucket =>
      val space = createStorageSpace
      val (bagRoot, bagInfo) = createS3BagWith(
        bucket,
        space = space,
        payloadFileCount = payloadFileCount
      )

      val ingestStep =
        withVerifier(bucket) {
          _.verify(
            ingestId = createIngestID,
            root = bagRoot,
            space = space,
            externalIdentifier = bagInfo.externalIdentifier
          )
        }

      val result = ingestStep.success.get

      result shouldBe a[IngestStepSucceeded[_]]
      result.summary shouldBe a[VerificationSuccessSummary]

      val summary = result.summary
        .asInstanceOf[VerificationSuccessSummary]
      val fixityListResult = summary.fixityListResult.value

      verifySuccessCount(
        fixityListResult.locations,
        expectedCount = expectedFileCount
      )
    }
  }

  it("fails a bag with an incorrect checksum in the file manifest") {
    val badBuilder = new S3BagBuilder {
      override protected def createPayloadManifest(
        entries: Seq[PayloadEntry]
      ): Option[String] =
        super.createPayloadManifest(
          entries.head.copy(contents = randomAlphanumeric) +: entries.tail
        )
    }

    assertBagFails(badBuilder) {
      case (ingestFailed, summary) =>
        val fixityListResult = summary.fixityListResult.value

        verifySuccessCount(
          fixityListResult.correct,
          expectedCount = expectedFileCount - 1
        )
        fixityListResult.errors should have size 1

        val fixityError = fixityListResult.errors.head
        val error = fixityError.e

        error shouldBe a[FailedChecksumNoMatch]
        error.getMessage should include("Checksum values do not match!")

        ingestFailed.maybeUserFacingMessage.get should startWith(
          "Unable to verify one file in the bag:"
        )
    }
  }

  it("fails a bag with an incorrect checksum in the tag manifest") {
    val badBuilder = new S3BagBuilder {
      override protected def createTagManifest(
        entries: Seq[ManifestFile]
      ): Option[String] =
        super.createTagManifest(
          entries.head.copy(contents = randomAlphanumeric) +: entries.tail
        )
    }

    assertBagFails(badBuilder) {
      case (_, summary) =>
        val fixityListResult = summary.fixityListResult.value

        verifySuccessCount(
          fixityListResult.correct,
          expectedCount = expectedFileCount - 1
        )
        fixityListResult.errors should have size 1

        val fixityError = fixityListResult.errors.head
        val error = fixityError.e

        error shouldBe a[FailedChecksumNoMatch]
        error.getMessage should include("Checksum values do not match!")
    }
  }

  it("fails a bag with multiple incorrect checksums in the file manifest") {
    val badBuilder = new S3BagBuilder {
      override protected def createPayloadManifest(
        entries: Seq[PayloadEntry]
      ): Option[String] =
        super.createPayloadManifest(
          entries.map { _.copy(contents = randomAlphanumeric) }
        )
    }

    assertBagFails(badBuilder) {
      case (ingestFailed, _) =>
        ingestFailed.maybeUserFacingMessage.get should startWith(
          s"Unable to verify $payloadFileCount files in the bag:"
        )
    }
  }

  it("fails a bag if the file manifest refers to a non-existent file") {
    val badBuilder = new S3BagBuilder {
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

    assertBagFails(badBuilder) {
      case (_, summary) =>
        val fixityListResult = summary.fixityListResult.value

        verifySuccessCount(
          fixityListResult.correct,
          expectedCount = expectedFileCount - 1
        )
        fixityListResult.errors should have size 1

        val fixityError = fixityListResult.errors.head
        val error = fixityError.e

        error shouldBe a[LocationNotFound[_]]
        error.getMessage should startWith("Location not available!")
    }
  }

  it("fails a bag if the file manifest does not exist") {
    val badBuilder = new S3BagBuilder {
      override protected def createPayloadManifest(
        entries: Seq[PayloadEntry]
      ): Option[String] =
        None
    }

    assertBagIncomplete(badBuilder) {
      case (ingestFailed, summary) =>
        val error = summary.e

        error shouldBe a[BagUnavailable]
        error.getMessage should include("Error loading manifest-sha256.txt")

        ingestFailed.maybeUserFacingMessage.get shouldBe "Error loading manifest-sha256.txt: no such file!"
    }
  }

  it("fails a bag if the tag manifest does not exist") {
    val badBuilder = new S3BagBuilder {
      override protected def createTagManifest(
        entries: Seq[ManifestFile]
      ): Option[String] =
        None
    }

    assertBagIncomplete(badBuilder) {
      case (ingestFailed, summary) =>
        val error = summary.e

        error shouldBe a[BagUnavailable]
        error.getMessage should include("Error loading tagmanifest-sha256.txt")

        ingestFailed.maybeUserFacingMessage.get shouldBe "Error loading tagmanifest-sha256.txt: no such file!"
    }
  }

  it("fails if the external identifier in the bag-info.txt is incorrect") {
    val space = createStorageSpace
    val externalIdentifier = randomAlphanumeric
    val bagInfoExternalIdentifier =
      ExternalIdentifier(externalIdentifier + "_bag-info")
    val payloadExternalIdentifier =
      ExternalIdentifier(externalIdentifier + "_payload")

    withLocalS3Bucket { bucket =>
      val (bagRoot, _) = createS3BagWith(
        bucket,
        externalIdentifier = bagInfoExternalIdentifier
      )

      val ingestStep =
        withVerifier(bucket) {
          _.verify(
            ingestId = createIngestID,
            root = bagRoot,
            space = space,
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
      val badBuilder = new S3BagBuilder {
        override protected def createFetchFile(
          entries: Seq[PayloadEntry]
        )(implicit namespace: String): Option[String] =
          super.createFetchFile(
            entries :+ entries.head.copy(
              bagPath = BagPath(entries.head.bagPath + "_extra"),
              path = entries.head.path + "_extra",
              contents = randomAlphanumeric
            )
          )
      }

      assertBagIncomplete(badBuilder) {
        case (ingestFailed, _) =>
          ingestFailed.e.getMessage should startWith(
            "fetch.txt refers to paths that aren't in the bag manifest: "
          )

          ingestFailed.maybeUserFacingMessage.get should startWith(
            "fetch.txt refers to paths that aren't in the bag manifest: "
          )
      }
    }

    it("fails if the fetch file refers to a file with the wrong URI scheme") {
      val wrongSchemeBuilder = new S3BagBuilder {
        override protected def buildFetchEntryLine(
          entry: PayloadEntry
        )(implicit namespace: String): String =
          super.buildFetchEntryLine(entry).replace("s3://", "none://")

        override protected def getFetchEntryCount(payloadFileCount: Int): Int =
          payloadFileCount
      }

      assertBagIncomplete(wrongSchemeBuilder) {
        case (ingestFailed, _) =>
          ingestFailed.e.getMessage should startWith(
            "fetch.txt refers to paths in a mismatched prefix or with a non-S3 URI scheme:"
          )

          ingestFailed.maybeUserFacingMessage.get should startWith(
            "fetch.txt refers to paths in a mismatched prefix or with a non-S3 URI scheme:"
          )
      }
    }

    it("fails if the fetch file refers to a file in a different namespace") {
      val wrongBucketFetchBuilder = new S3BagBuilder {
        override protected def buildFetchEntryLine(
          entry: PayloadEntry
        )(implicit namespace: String): String =
          super.buildFetchEntryLine(entry)(namespace = s"wrong-$namespace")

        override protected def getFetchEntryCount(payloadFileCount: Int): Int =
          payloadFileCount
      }

      assertBagIncomplete(wrongBucketFetchBuilder) {
        case (ingestFailed, _) =>
          ingestFailed.e.getMessage should startWith(
            "fetch.txt refers to paths in a mismatched prefix or with a non-S3 URI scheme:"
          )

          ingestFailed.maybeUserFacingMessage.get should startWith(
            "fetch.txt refers to paths in a mismatched prefix or with a non-S3 URI scheme:"
          )
      }
    }

    it("fails if the fetch file refers to a file in the wrong space") {
      val bagSpaceFetchBuilder = new S3BagBuilder {
        override protected def buildFetchEntryLine(
          entry: PayloadEntry
        )(implicit namespace: String): String =
          super.buildFetchEntryLine(
            entry.copy(
              path = "badspace_" + entry.path
            )
          )

        override protected def getFetchEntryCount(payloadFileCount: Int): Int =
          payloadFileCount
      }

      assertBagIncomplete(bagSpaceFetchBuilder) {
        case (ingestFailed, _) =>
          ingestFailed.e.getMessage should startWith(
            "fetch.txt refers to paths in a mismatched prefix or with a non-S3 URI scheme:"
          )

          ingestFailed.maybeUserFacingMessage.get should startWith(
            "fetch.txt refers to paths in a mismatched prefix or with a non-S3 URI scheme:"
          )
      }
    }

    it(
      "fails if the fetch file refers to a file with the wrong external identifier"
    ) {
      val badExternalIdentifierFetchBuilder = new S3BagBuilder {
        override protected def buildFetchEntryLine(
          entry: PayloadEntry
        )(implicit namespace: String): String =
          super.buildFetchEntryLine(
            entry.copy(
              path = entry.path.replace("/", "/wrong_")
            )
          )

        override protected def getFetchEntryCount(payloadFileCount: Int): Int =
          payloadFileCount
      }

      assertBagIncomplete(badExternalIdentifierFetchBuilder) {
        case (ingestFailed, _) =>
          ingestFailed.e.getMessage should startWith(
            "fetch.txt refers to paths in a mismatched prefix or with a non-S3 URI scheme:"
          )

          ingestFailed.maybeUserFacingMessage.get should startWith(
            "fetch.txt refers to paths in a mismatched prefix or with a non-S3 URI scheme:"
          )
      }
    }
  }

  describe("checks for unreferenced files") {
    it("fails if there is one unreferenced file") {
      val badBuilder = new S3BagBuilder {
        override def createS3BagWith(
          bucket: Bucket,
          space: StorageSpace,
          externalIdentifier: ExternalIdentifier,
          payloadFileCount: Int
        ): (S3ObjectLocationPrefix, BagInfo) = {
          val (bagRoot, bagInfo) =
            super.createS3BagWith(
              bucket = bucket,
              space = space,
              externalIdentifier = externalIdentifier,
              payloadFileCount = payloadFileCount
            )

          val location = bagRoot.asLocation("unreferencedfile.txt")
          s3Client.putObject(
            location.bucket,
            location.key,
            randomAlphanumeric
          )

          (bagRoot, bagInfo)
        }
      }

      assertBagIncomplete(badBuilder) {
        case (ingestFailed, _) =>
          ingestFailed.e.getMessage should startWith(
            "Bag contains a file which is not referenced in the manifest:"
          )

          ingestFailed.maybeUserFacingMessage.get shouldBe
            "Bag contains a file which is not referenced in the manifest: /unreferencedfile.txt"
      }
    }

    it("fails if there are multiple unreferenced files") {
      val badBuilder = new S3BagBuilder {
        override def createS3BagWith(
          bucket: Bucket,
          space: StorageSpace,
          externalIdentifier: ExternalIdentifier,
          payloadFileCount: Int
        ): (S3ObjectLocationPrefix, BagInfo) = {
          val (bagRoot, bagInfo) =
            super.createS3BagWith(
              bucket = bucket,
              space = space,
              externalIdentifier = externalIdentifier,
              payloadFileCount = payloadFileCount
            )

          (1 to 3).foreach { i =>
            val location = bagRoot.asLocation(s"unreferencedfile_$i.txt")

            s3Client.putObject(
              location.bucket,
              location.key,
              randomAlphanumeric
            )
          }

          (bagRoot, bagInfo)
        }
      }

      assertBagIncomplete(badBuilder) {
        case (ingestFailed, _) =>
          ingestFailed.e.getMessage should startWith(
            "Bag contains 3 files which are not referenced in the manifest:"
          )

          ingestFailed.maybeUserFacingMessage.get shouldBe
            s"Bag contains 3 files which are not referenced in the manifest: " +
              "/unreferencedfile_1.txt, /unreferencedfile_2.txt, /unreferencedfile_3.txt"
      }
    }

    it("fails if a file in the fetch.txt also appears in the bag") {
      val alwaysWriteAsFetchBuilder = new S3BagBuilder {
        override protected def getFetchEntryCount(payloadFileCount: Int): Int =
          payloadFileCount

        override def createS3BagWith(
          bucket: Bucket,
          space: StorageSpace,
          externalIdentifier: ExternalIdentifier,
          payloadFileCount: Int
        ): (S3ObjectLocationPrefix, BagInfo) = {
          val (bagRoot, bagInfo) =
            super.createS3BagWith(
              bucket = bucket,
              space = space,
              externalIdentifier,
              payloadFileCount = payloadFileCount
            )

          val bag = new S3BagReader().get(bagRoot).right.value

          // Write one of the fetch.txt entries as a concrete file
          val badFetchPath: BagPath = bag.fetch.get.paths.head
          val badFetchLocation = bagRoot.asLocation(badFetchPath.value)

          s3Client.putObject(
            badFetchLocation.bucket,
            badFetchLocation.key,
            randomAlphanumeric
          )

          (bagRoot, bagInfo)
        }
      }

      assertBagIncomplete(alwaysWriteAsFetchBuilder) {
        case (ingestFailed, _) =>
          ingestFailed.e.getMessage should startWith(
            "Files referred to in the fetch.txt also appear in the bag:"
          )

          ingestFailed.maybeUserFacingMessage.get should startWith(
            "Files referred to in the fetch.txt also appear in the bag:"
          )
      }
    }

    it("passes a bag that includes an extra manifest/tag manifest") {
      withLocalS3Bucket { bucket =>
        val space = createStorageSpace
        val (bagRoot, bagInfo) = createS3BagWith(bucket, space = space)

        val location = bagRoot.asLocation("tagmanifest-sha512.txt")

        s3Client.putObject(
          location.bucket,
          location.key,
          "<empty SHA512 tag manifest>"
        )

        val ingestStep =
          withVerifier(bucket) {
            _.verify(
              ingestId = createIngestID,
              root = bagRoot,
              space = space,
              externalIdentifier = bagInfo.externalIdentifier
            )
          }

        ingestStep.success.get shouldBe a[IngestStepSucceeded[_]]
      }
    }
  }

  describe("checks the Payload-Oxum") {
    it("fails if the Payload-Oxum has the wrong file count") {
      val badBuilder = new S3BagBuilder {
        override protected def createPayloadOxum(
          entries: Seq[PayloadEntry]
        ): PayloadOxum = {
          val oxum = super.createPayloadOxum(entries)

          oxum.copy(numberOfPayloadFiles = oxum.numberOfPayloadFiles - 1)
        }
      }

      assertBagIncomplete(badBuilder) {
        case (ingestFailed, _) =>
          ingestFailed.maybeUserFacingMessage.get shouldBe
            s"""Payload-Oxum has the wrong number of payload files: ${payloadFileCount - 1}, but bag manifest has $payloadFileCount"""
      }
    }

    it("fails if the Payload-Oxum has the wrong octet count") {
      val badBuilder = new S3BagBuilder {
        override protected def createPayloadOxum(
          entries: Seq[PayloadEntry]
        ): PayloadOxum = {
          val oxum = super.createPayloadOxum(entries)

          oxum.copy(payloadBytes = oxum.payloadBytes - 1)
        }
      }

      assertBagIncomplete(badBuilder) {
        case (ingestFailed, _) =>
          ingestFailed.maybeUserFacingMessage.get should fullyMatch regex
            s"""Payload-Oxum has the wrong octetstream sum: \\d+ bytes, but bag actually contains \\d+ bytes"""
      }
    }
  }

  private def verifySuccessCount(
    successes: List[FileFixityCorrect[_]],
    expectedCount: Int
  ): Assertion =
    if (successes
          .map { _.objectLocation.asInstanceOf[S3ObjectLocation].key }
          .exists {
            _.endsWith("/fetch.txt")
          }) {
      successes.size shouldBe expectedCount + 1
    } else {
      successes.size shouldBe expectedCount
    }

  // Given a builder that fails to create a valid bag for some reason, ensure that
  // it is caught correctly by the verifier.
  private def assertBagResultFails(
    badBuilder: S3BagBuilder
  )(assertion: IngestStepResult[VerificationSummary] => Assertion): Assertion =
    withLocalS3Bucket { bucket =>
      val space = createStorageSpace
      val (bagRoot, bagInfo) = badBuilder.createS3BagWith(
        bucket,
        space = space,
        payloadFileCount = payloadFileCount
      )

      val ingestStep =
        withVerifier(bucket) {
          _.verify(
            ingestId = createIngestID,
            root = bagRoot,
            space = space,
            externalIdentifier = bagInfo.externalIdentifier
          )
        }

      val result = ingestStep.success.get
      debug(s"result = $result")

      result shouldBe a[IngestFailed[_]]
      assertion(result)
    }

  private def assertBagFails(badBuilder: S3BagBuilder)(
    assertion: (
      IngestFailed[VerificationFailureSummary],
      VerificationFailureSummary
    ) => Assertion
  ): Assertion =
    assertBagResultFails(badBuilder) { result =>
      result.summary shouldBe a[VerificationFailureSummary]

      val failedResult =
        result.asInstanceOf[IngestFailed[VerificationFailureSummary]]
      val summary = result.summary.asInstanceOf[VerificationFailureSummary]

      assertion(failedResult, summary)
    }

  private def assertBagIncomplete(badBuilder: S3BagBuilder)(
    assertion: (
      IngestFailed[VerificationIncompleteSummary],
      VerificationIncompleteSummary
    ) => Assertion
  ): Assertion =
    assertBagResultFails(badBuilder) { result =>
      result.summary shouldBe a[VerificationIncompleteSummary]

      val failedResult =
        result.asInstanceOf[IngestFailed[VerificationIncompleteSummary]]
      val summary = result.summary.asInstanceOf[VerificationIncompleteSummary]

      assertion(failedResult, summary)
    }
}

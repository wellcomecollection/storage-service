package uk.ac.wellcome.platform.archive.bagverifier.services

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, OptionValues, TryValues}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagverifier.fixity.{FailedChecksumNoMatch, FileFixityCorrect}
import uk.ac.wellcome.platform.archive.bagverifier.models.{VerificationFailureSummary, VerificationSuccessSummary, VerificationSummary}
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagVersion, ExternalIdentifier}
import uk.ac.wellcome.platform.archive.common.fixtures.{BagBuilder, PayloadEntry}
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestFailed, IngestStepResult, IngestStepSucceeded, StorageSpace}
import uk.ac.wellcome.storage.store.TypedStore
import uk.ac.wellcome.storage.store.fixtures.NamespaceFixtures
import uk.ac.wellcome.storage.{Location, Prefix, S3ObjectLocation}

trait BagVerifierTestCases[BagLocation <: Location, BagPrefix <: Prefix[BagLocation], Namespace]
  extends AnyFunSpec
    with Matchers
    with OptionValues
    with TryValues
    with BagBuilder[BagLocation, BagPrefix, Namespace]
    with NamespaceFixtures[BagLocation, Namespace] {

  def withTypedStore[R](testWith: TestWith[TypedStore[BagLocation, String], R]): R

  def withVerifier[R](namespace: Namespace)(
    testWith: TestWith[BagVerifier[BagLocation, BagPrefix], R]
  )(implicit typedStore: TypedStore[BagLocation, String]): R

  val payloadFileCount: Int = randomInt(from = 1, to = 10)

  val expectedFileCount: Int = payloadFileCount + List(
    "manifest-sha256.txt",
    "bagit.txt",
    "bag-info.txt"
  ).size

  def createBagRootImpl(
    space: StorageSpace,
    externalIdentifier: ExternalIdentifier,
    version: BagVersion)(
    implicit namespace: Namespace): BagPrefix

  def createBagLocationImpl(bagRoot: BagPrefix, path: String): BagLocation

  def buildFetchEntryLineImpl(entry: PayloadEntry)(implicit namespace: Namespace): String

  trait BagBuilderImpl extends BagBuilder[BagLocation, BagPrefix, Namespace] {
    override def createBagRoot(
      space: StorageSpace,
      externalIdentifier: ExternalIdentifier,
      version: BagVersion)(
      implicit namespace: Namespace
    ): BagPrefix =
      createBagRootImpl(space, externalIdentifier, version)

    override def createBagLocation(bagRoot: BagPrefix, path: String): BagLocation =
      createBagLocationImpl(bagRoot, path)

    override def buildFetchEntryLine(entry: PayloadEntry)(implicit namespace: Namespace): String =
      buildFetchEntryLineImpl(entry)
  }

  it("accepts a correctly-formatted bag") {
    withNamespace { implicit namespace =>
      withTypedStore { implicit typedStore =>
        val space = createStorageSpace

        val (bagObjects, bagRoot, bagInfo) = createBagContentsWith(
          space = space,
          payloadFileCount = payloadFileCount
        )
        uploadBagObjects(bagObjects)

        val ingestStep =
          withVerifier(namespace) {
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
  }

  it("fails a bag with an incorrect checksum in the file manifest") {
    val badBuilder: BagBuilderImpl = new BagBuilderImpl {
      override protected def createPayloadManifest(
        entries: Seq[PayloadEntry]
      ): Option[String] =
        super.createPayloadManifest(
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

  private def assertBagFails(badBuilder: BagBuilderImpl)(
    assertion: (
      IngestFailed[VerificationFailureSummary],
        VerificationFailureSummary
      ) => Assertion
  ): Assertion =
    assertBagResultFails(badBuilder) { result =>
      println(s"@@AWLC result = $result")
      result.summary shouldBe a[VerificationFailureSummary]

      val failedResult =
        result.asInstanceOf[IngestFailed[VerificationFailureSummary]]
      val summary = result.summary.asInstanceOf[VerificationFailureSummary]

      assertion(failedResult, summary)
    }

  // Given a builder that fails to create a valid bag for some reason, ensure that
  // it is caught correctly by the verifier.
  private def assertBagResultFails(
    badBuilder: BagBuilderImpl
  )(assertion: IngestStepResult[VerificationSummary] => Assertion): Assertion =
    withNamespace { implicit namespace =>
      withTypedStore { implicit typedStore =>
        val space = createStorageSpace
        val (bagObjects, bagRoot, bagInfo) = badBuilder.createBagContentsWith(
          space = space,
          payloadFileCount = payloadFileCount
        )
        uploadBagObjects(bagObjects)

        val ingestStep =
          withVerifier(namespace) {
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
    }

}

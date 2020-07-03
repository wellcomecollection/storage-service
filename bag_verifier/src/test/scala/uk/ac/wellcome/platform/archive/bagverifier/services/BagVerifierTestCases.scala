package uk.ac.wellcome.platform.archive.bagverifier.services

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, TryValues}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagverifier.fixity.FileFixityCorrect
import uk.ac.wellcome.platform.archive.bagverifier.models.VerificationSuccessSummary
import uk.ac.wellcome.platform.archive.common.fixtures.BagBuilder
import uk.ac.wellcome.platform.archive.common.storage.models.IngestStepSucceeded
import uk.ac.wellcome.storage.store.TypedStore
import uk.ac.wellcome.storage.store.fixtures.NamespaceFixtures
import uk.ac.wellcome.storage.{Location, Prefix, S3ObjectLocation}

trait BagVerifierTestCases[BagLocation <: Location, BagPrefix <: Prefix[BagLocation], Namespace]
  extends AnyFunSpec
    with Matchers
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
}

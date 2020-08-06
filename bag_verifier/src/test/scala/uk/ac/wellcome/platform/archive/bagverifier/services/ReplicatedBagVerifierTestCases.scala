package uk.ac.wellcome.platform.archive.bagverifier.services

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagverifier.models.{ReplicatedBagVerifyContext, VerificationIncompleteSummary}
import uk.ac.wellcome.platform.archive.common.storage.models.IngestFailed
import uk.ac.wellcome.storage.s3.S3ObjectLocationPrefix
import uk.ac.wellcome.storage.store.s3.S3TypedStore
import uk.ac.wellcome.storage.{Location, Prefix}

trait ReplicatedBagVerifierTestCases[
  ReplicaBagLocation <: Location,
  ReplicaBagPrefix <: Prefix[ReplicaBagLocation],
  ReplicaNamespace
] extends BagVerifierTestCases[
  ReplicatedBagVerifier[
    ReplicaBagLocation,
    ReplicaBagPrefix
  ],
  ReplicatedBagVerifyContext[ReplicaBagPrefix],
  ReplicaBagLocation,
  ReplicaBagPrefix,
  ReplicaNamespace
] {
  override def withBagContext[R](srcBagRoot: S3ObjectLocationPrefix, replicaBagRoot: ReplicaBagPrefix)(testWith: TestWith[ReplicatedBagVerifyContext[ReplicaBagPrefix], R]): R =
    testWith(ReplicatedBagVerifyContext(srcBagRoot, replicaBagRoot))

  it("fails a bag if it doesn't match original tag manifest") {

    val space = createStorageSpace
    val externalIdentifier = createExternalIdentifier
    withTypedStore { implicit typedStore =>
      withBag(space, externalIdentifier)() { case (srcBucket, replicaNamespace, srcBagRoot, replicaBagRoot) =>
        S3TypedStore[String].put(srcBagRoot.asLocation("tagmanifest-sha256.txt"))(randomAlphanumeric)
        val ingestStep =
          withVerifier(srcBucket) {
            _.verify(
              ingestId = createIngestID,
              bagContext = ReplicatedBagVerifyContext(
                srcRoot = srcBagRoot,
                replicaRoot = replicaBagRoot
              ),
              space = space,
              externalIdentifier = externalIdentifier
            )
          }

        val result = ingestStep.success.get

        result shouldBe a[IngestFailed[_]]
        result.summary shouldBe a[VerificationIncompleteSummary]

        result.maybeUserFacingMessage shouldNot be(defined)
      }
    }
  }

  it("fails a bag if it cannot read the original bag") {
    val space = createStorageSpace
    val externalIdentifier = createExternalIdentifier
    withTypedStore { implicit typedStore =>
      withBag(space, externalIdentifier)() { case (srcBucket, replicaNamespace, srcBagRoot, replicaBagRoot) =>

        val ingestStep =
          withVerifier(srcBucket) {
            _.verify(
              ingestId = createIngestID,
              bagContext = ReplicatedBagVerifyContext(
                srcRoot = srcBagRoot,
                replicaRoot = replicaBagRoot
              ),
              space = space,
              externalIdentifier = externalIdentifier
            )
          }

        val result = ingestStep.success.get

        result shouldBe a[IngestFailed[_]]
        result.summary shouldBe a[VerificationIncompleteSummary]

        result.maybeUserFacingMessage shouldNot be(defined)
      }
    }
  }
}

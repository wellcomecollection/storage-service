package uk.ac.wellcome.platform.archive.bagverifier.services

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagverifier.models.{
  ReplicatedBagVerifyContext,
  VerificationIncompleteSummary
}
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
  override def withBagContext[R](replicaBagRoot: ReplicaBagPrefix)(testWith: TestWith[ReplicatedBagVerifyContext[ReplicaBagPrefix], R]): R =
    withLocalS3Bucket { srcBucket =>
      withTypedStore { implicit typedStore =>
        val srcBagRoot = S3ObjectLocationPrefix(
          bucket = srcBucket.name,
          keyPrefix = replicaBagRoot.pathPrefix
        )

        val _ = for {
          tagManifestContents <- typedStore.get(replicaBagRoot.asLocation("tagmanifest-sha256.txt"))
          _ <- S3TypedStore[String].put(srcBagRoot.asLocation("tagmanifest-sha256.txt"))(tagManifestContents.identifiedT)
        } yield ()

        testWith(ReplicatedBagVerifyContext(srcBagRoot, replicaBagRoot))
      }
    }

  it("fails a bag if it doesn't match original tag manifest") {
    val space = createStorageSpace
    val externalIdentifier = createExternalIdentifier

    withLocalS3Bucket { srcBucket =>
        withBag(space, externalIdentifier) { case (primaryBucket, replicaBagRoot) =>
          val srcBagRoot = S3ObjectLocationPrefix(srcBucket.name, replicaBagRoot.pathPrefix)
          S3TypedStore[String].put(srcBagRoot.asLocation("tagmanifest-sha256.txt"))(randomAlphanumeric)
          val ingestStep =
            withVerifier(primaryBucket) {
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

    withLocalS3Bucket { srcBucket =>
        withBag(space, externalIdentifier) { case (primaryBucket, replicaBagRoot) =>
          val srcBagRoot = S3ObjectLocationPrefix(srcBucket.name, replicaBagRoot.pathPrefix)
          val ingestStep =
            withVerifier(primaryBucket) {
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

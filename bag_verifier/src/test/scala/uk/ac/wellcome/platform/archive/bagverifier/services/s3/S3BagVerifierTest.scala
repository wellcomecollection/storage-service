package uk.ac.wellcome.platform.archive.bagverifier.services.s3

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagverifier.models.{
  BagVerifyContext,
  ReplicatedBagVerifyContext,
  StandaloneBagVerifyContext
}
import uk.ac.wellcome.platform.archive.bagverifier.services._
import uk.ac.wellcome.platform.archive.common.bagit.services.BagReader
import uk.ac.wellcome.platform.archive.common.bagit.services.s3.S3BagReader
import uk.ac.wellcome.platform.archive.common.fixtures.BagBuilder
import uk.ac.wellcome.platform.archive.common.fixtures.s3.S3BagBuilder
import uk.ac.wellcome.platform.archive.common.storage.models.EnsureTrailingSlash
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.storage.store.TypedStore
import uk.ac.wellcome.storage.store.s3.S3TypedStore

trait S3BagVerifierTests[Verifier <: BagVerifier[
  BagContext,
  S3ObjectLocation,
  S3ObjectLocationPrefix
], BagContext <: BagVerifyContext[S3ObjectLocationPrefix]]
    extends S3BagBuilder {
  this: BagVerifierTestCases[
    Verifier,
    BagContext,
    S3ObjectLocation,
    S3ObjectLocationPrefix,
    Bucket
  ] =>
  override implicit val et: EnsureTrailingSlash[S3ObjectLocationPrefix] =
    EnsureTrailingSlash.s3PrefixTrailingSlash

  override def withTypedStore[R](
    testWith: TestWith[TypedStore[S3ObjectLocation, String], R]
  ): R =
    testWith(S3TypedStore[String])

  override def withNamespace[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  override def createId(implicit bucket: Bucket): S3ObjectLocation =
    createS3ObjectLocationWith(bucket)

  override def writeFile(location: S3ObjectLocation, contents: String): Unit =
    s3Client.putObject(location.bucket, location.key, contents)

  override def createBagReader
    : BagReader[S3ObjectLocation, S3ObjectLocationPrefix] =
    new S3BagReader()
}

class S3ReplicatedBagVerifierTest
    extends ReplicatedBagVerifierTestCases[
      S3ObjectLocation,
      S3ObjectLocationPrefix,
      Bucket
    ]
    with S3BagVerifierTests[
      ReplicatedBagVerifier[
        S3ObjectLocation,
        S3ObjectLocationPrefix
      ],
      ReplicatedBagVerifyContext[S3ObjectLocationPrefix]
    ] {
  override def withVerifier[R](primaryBucket: Bucket)(
    testWith: TestWith[
      ReplicatedBagVerifier[
        S3ObjectLocation,
        S3ObjectLocationPrefix
      ],
      R
    ]
  ): R =
    testWith(
      new S3ReplicatedBagVerifier(primaryBucket = primaryBucket.name)
    )

  override val bagBuilder
    : BagBuilder[S3ObjectLocation, S3ObjectLocationPrefix, Bucket] =
    new S3BagBuilder {}

}

class S3StandaloneBagVerifierTest
    extends BagVerifierTestCases[
      S3StandaloneBagVerifier,
      StandaloneBagVerifyContext,
      S3ObjectLocation,
      S3ObjectLocationPrefix,
      Bucket
    ]
    with S3BagVerifierTests[
      S3StandaloneBagVerifier,
      StandaloneBagVerifyContext
    ] {
  override def withVerifier[R](
    primaryBucket: Bucket
  )(testWith: TestWith[S3StandaloneBagVerifier, R]): R =
    testWith(
      new S3StandaloneBagVerifier(primaryBucket = primaryBucket.name)
    )

  override val bagBuilder
    : BagBuilder[S3ObjectLocation, S3ObjectLocationPrefix, Bucket] =
    new S3BagBuilder {}

  override def withBagContext[R](bagRoot: S3ObjectLocationPrefix)(
    testWith: TestWith[StandaloneBagVerifyContext, R]
  ): R = testWith(StandaloneBagVerifyContext(bagRoot))
}

package weco.storage_service.bag_verifier.services.s3

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import weco.fixtures.TestWith
import weco.storage_service.bag_verifier.models.{BagVerifyContext, ReplicatedBagVerifyContext, StandaloneBagVerifyContext}
import weco.storage_service.bag_verifier.services._
import weco.storage_service.bagit.services.BagReader
import weco.storage_service.bagit.services.s3.S3BagReader
import weco.storage_service.fixtures.BagBuilder
import weco.storage_service.fixtures.s3.S3BagBuilder
import weco.storage_service.storage.models.EnsureTrailingSlash
import weco.storage.fixtures.S3Fixtures.Bucket
import weco.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import weco.storage.store.TypedStore
import weco.storage.store.s3.S3TypedStore

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
    putString(location, contents)

  override def createBagReader
    : BagReader[S3ObjectLocation, S3ObjectLocationPrefix] =
    new S3BagReader()

  override val bagBuilder
    : BagBuilder[S3ObjectLocation, S3ObjectLocationPrefix, Bucket] =
    new S3BagBuilder {}

  implicit val amazonS3: AmazonS3 =
    AmazonS3ClientBuilder.standard()
      .withCredentials(new AWSStaticCredentialsProvider(
        new BasicAWSCredentials("accessKey1", "verySecretKey1")))
      .withPathStyleAccessEnabled(true)
      .withEndpointConfiguration(new EndpointConfiguration("http://localhost:33333", "localhost"))
      .build()
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
      S3BagVerifier.replicated(primaryBucket = primaryBucket.name)
    )
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
      S3BagVerifier.standalone(primaryBucket = primaryBucket.name)
    )

  override def withBagContext[R](bagRoot: S3ObjectLocationPrefix)(
    testWith: TestWith[StandaloneBagVerifyContext, R]
  ): R = testWith(StandaloneBagVerifyContext(bagRoot))
}

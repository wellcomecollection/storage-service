package uk.ac.wellcome.platform.archive.bagverifier.services.s3

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagverifier.models.{
  BagVerifyContext,
  ReplicatedBagVerifyContext,
  StandaloneBagVerifyContext
}
import uk.ac.wellcome.platform.archive.bagverifier.services._
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagVersion,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.bagit.services.BagReader
import uk.ac.wellcome.platform.archive.common.bagit.services.s3.S3BagReader
import uk.ac.wellcome.platform.archive.common.fixtures.{
  BagBuilder,
  PayloadEntry
}
import uk.ac.wellcome.platform.archive.common.fixtures.s3.S3BagBuilder
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.store.TypedStore
import uk.ac.wellcome.storage.store.s3.S3TypedStore
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}

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

  override def createBagRootImpl(
    space: StorageSpace,
    externalIdentifier: ExternalIdentifier,
    version: BagVersion
  )(
    implicit bucket: Bucket
  ): S3ObjectLocationPrefix =
    createBagRoot(
      space = space,
      externalIdentifier = externalIdentifier,
      version = version
    )

  override def createBagLocationImpl(
    bagRoot: S3ObjectLocationPrefix,
    path: String
  ): S3ObjectLocation =
    createBagLocation(bagRoot, path = path)

  override def buildFetchEntryLineImpl(
    entry: PayloadEntry
  )(implicit bucket: Bucket): String =
    buildFetchEntryLine(entry)

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
      Bucket,
      S3ObjectLocation,
      S3ObjectLocationPrefix,
      Bucket
    ]
    with S3BagVerifierTests[
      ReplicatedBagVerifier[
        S3ObjectLocation,
        S3ObjectLocationPrefix,
        S3ObjectLocation,
        S3ObjectLocationPrefix
      ],
      ReplicatedBagVerifyContext[S3ObjectLocationPrefix, S3ObjectLocationPrefix]
    ] {
  override def withVerifier[R](primaryBucket: Bucket)(
    testWith: TestWith[
      ReplicatedBagVerifier[
        S3ObjectLocation,
        S3ObjectLocationPrefix,
        S3ObjectLocation,
        S3ObjectLocationPrefix
      ],
      R
    ]
  )(
    implicit typedStore: TypedStore[S3ObjectLocation, String]
  ): R =
    testWith(
      new S3ReplicatedBagVerifier(primaryBucket = primaryBucket.name)
    )

  override protected def copyTagManifest(
    srcRoot: S3ObjectLocationPrefix,
    replicaRoot: S3ObjectLocationPrefix
  ): Unit =
    s3Client.copyObject(
      replicaRoot.bucket,
      s"${replicaRoot.keyPrefix}/tagmanifest-sha256.txt",
      srcRoot.bucket,
      s"${srcRoot.keyPrefix}/tagmanifest-sha256.txt"
    )

  override def createSrcPrefix(
    implicit bucket: Bucket
  ): S3ObjectLocationPrefix =
    createS3ObjectLocationPrefixWith(bucket)

  override def createReplicaPrefix(
    implicit bucket: Bucket
  ): S3ObjectLocationPrefix =
    createS3ObjectLocationPrefixWith(bucket)

  override def withSrcNamespace[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  override def withReplicaNamespace[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  override def withSrcTypedStore[R](
    testWith: TestWith[TypedStore[S3ObjectLocation, String], R]
  ): R =
    testWith(S3TypedStore[String])

  override def withReplicaTypedStore[R](
    testWith: TestWith[TypedStore[S3ObjectLocation, String], R]
  ): R =
    testWith(S3TypedStore[String])

  override val srcBagBuilder
    : BagBuilder[S3ObjectLocation, S3ObjectLocationPrefix, Bucket] =
    new S3BagBuilder {}

  override val replicaBagBuilder
    : BagBuilder[S3ObjectLocation, S3ObjectLocationPrefix, Bucket] =
    new S3BagBuilder {}
}

class S3StandaloneBagVerifierTest
    extends StandaloneBagVerifierTestCases[
      S3ObjectLocation,
      S3ObjectLocationPrefix,
      Bucket
    ]
    with S3BagVerifierTests[
      StandaloneBagVerifier[S3ObjectLocation, S3ObjectLocationPrefix],
      StandaloneBagVerifyContext[S3ObjectLocationPrefix]
    ] {
  override def withVerifier[R](primaryBucket: Bucket)(
    testWith: TestWith[
      StandaloneBagVerifier[S3ObjectLocation, S3ObjectLocationPrefix],
      R
    ]
  )(
    implicit typedStore: TypedStore[S3ObjectLocation, String]
  ): R =
    testWith(
      new S3StandaloneBagVerifier(primaryBucket = primaryBucket.name)
    )
}

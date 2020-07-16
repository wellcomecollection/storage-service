package uk.ac.wellcome.platform.archive.bagverifier.services.s3

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagverifier.services.{ReplicatedBagVerifier, ReplicatedBagVerifierTestCases}
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagVersion, ExternalIdentifier}
import uk.ac.wellcome.platform.archive.common.bagit.services.BagReader
import uk.ac.wellcome.platform.archive.common.bagit.services.s3.S3BagReader
import uk.ac.wellcome.platform.archive.common.fixtures.PayloadEntry
import uk.ac.wellcome.platform.archive.common.fixtures.s3.S3BagBuilder
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.store.TypedStore
import uk.ac.wellcome.storage.store.s3.NewS3TypedStore
import uk.ac.wellcome.storage.{S3ObjectLocation, S3ObjectLocationPrefix}

class S3ReplicatedBagVerifierTest
    extends ReplicatedBagVerifierTestCases[
      S3ObjectLocation,
      S3ObjectLocationPrefix,
      Bucket
    ]
    with S3BagBuilder {

  override def withTypedStore[R](
    testWith: TestWith[TypedStore[S3ObjectLocation, String], R]
  ): R =
    testWith(NewS3TypedStore[String])

  override def withVerifier[R](primaryBucket: Bucket)(
    testWith: TestWith[ReplicatedBagVerifier[S3ObjectLocation, S3ObjectLocationPrefix], R]
  )(
    implicit typedStore: TypedStore[S3ObjectLocation, String]
  ): R =
    testWith(
      new S3ReplicatedBagVerifier(primaryBucket = primaryBucket.name)
    )

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

  override def createBagReader(
    implicit typedStore: TypedStore[S3ObjectLocation, String]
  ): BagReader[S3ObjectLocation, S3ObjectLocationPrefix] =
    new S3BagReader()
}

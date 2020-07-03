package uk.ac.wellcome.platform.archive.bagverifier.services.s3

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagverifier.services.{BagVerifier, BagVerifierTestCases}
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagVersion, ExternalIdentifier}
import uk.ac.wellcome.platform.archive.common.fixtures.PayloadEntry
import uk.ac.wellcome.platform.archive.common.fixtures.s3.S3BagBuilder
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.store.TypedStore
import uk.ac.wellcome.storage.store.s3.NewS3TypedStore
import uk.ac.wellcome.storage.{S3ObjectLocation, S3ObjectLocationPrefix}

class S3BagVerifierTest
  extends BagVerifierTestCases[S3ObjectLocation, S3ObjectLocationPrefix, Bucket]
    with S3BagBuilder {

  override def withTypedStore[R](
      testWith: TestWith[TypedStore[S3ObjectLocation, String], R]): R =
    testWith(NewS3TypedStore[String])

  override def withVerifier[R](
    primaryBucket: Bucket)(
    testWith: TestWith[BagVerifier[S3ObjectLocation, S3ObjectLocationPrefix], R])(
    implicit typedStore: TypedStore[S3ObjectLocation, String]
  ): R =
    testWith(
      new S3BagVerifier(primaryBucket = primaryBucket.name)
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
    version: BagVersion)(
    implicit bucket: Bucket
  ): S3ObjectLocationPrefix =
    createBagRoot(
      space = space,
      externalIdentifier = externalIdentifier,
      version = version
    )

  override def createBagLocationImpl(bagRoot: S3ObjectLocationPrefix, path: String): S3ObjectLocation =
    createBagLocation(bagRoot, path = path)

  override def buildFetchEntryLineImpl(entry: PayloadEntry)(implicit bucket: Bucket): String =
    buildFetchEntryLine(entry)
}

package uk.ac.wellcome.platform.archive.common.bagit.services.s3

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.bagit.services.{BagReader, BagReaderTestCases}
import uk.ac.wellcome.platform.archive.common.fixtures.S3BagBuilderBase
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.storage.store.TypedStore
import uk.ac.wellcome.storage.store.s3.{S3StreamStore, S3TypedStore}

class S3BagReaderTest
  extends BagReaderTestCases[Unit, Bucket, S3ObjectLocation, S3ObjectLocationPrefix]
    with S3BagBuilderBase
    with S3Fixtures {

  override def withTypedStore[R](
    testWith: TestWith[TypedStore[S3ObjectLocation, String], R]
  )(implicit context: Unit): R = {
    implicit val s3StreamStore: S3StreamStore = new S3StreamStore()

    testWith(new S3TypedStore[String]())
  }

  override def withNamespace[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  override def deleteFile(prefix: S3ObjectLocationPrefix, path: String)(
    implicit context: Unit
  ): Unit =
    s3Client.deleteObject(
      prefix.bucket,
      prefix.asLocation(path).key
    )

  override def withContext[R](testWith: TestWith[Unit, R]): R = testWith(())

  override def withBagReader[R](
    testWith: TestWith[BagReader[S3ObjectLocation, S3ObjectLocationPrefix], R]
  )(implicit context: Unit): R =
    testWith(new S3BagReader())

  override protected def toString(bucket: Bucket): String = bucket.name
}

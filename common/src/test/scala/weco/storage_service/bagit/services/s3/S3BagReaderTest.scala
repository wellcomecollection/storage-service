package weco.storage_service.bagit.services.s3

import weco.fixtures.TestWith
import weco.storage_service.bagit.services.{BagReader, BagReaderTestCases}
import weco.storage.fixtures.S3Fixtures.Bucket
import weco.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import weco.storage.store.TypedStore
import weco.storage.store.s3.S3TypedStore
import weco.storage_service.fixtures.s3.S3BagBuilder

class S3BagReaderTest
    extends BagReaderTestCases[
      Unit,
      Bucket,
      S3ObjectLocation,
      S3ObjectLocationPrefix
    ]
    with S3BagBuilder {

  override def withTypedStore[R](
    testWith: TestWith[TypedStore[S3ObjectLocation, String], R]
  )(implicit context: Unit): R =
    testWith(S3TypedStore[String])

  override def withNamespace[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  override def deleteFile(root: S3ObjectLocationPrefix, path: String)(
    implicit context: Unit
  ): Unit =
    s3Client.deleteObject(root.bucket, root.asLocation(path).key)

  override def withContext[R](testWith: TestWith[Unit, R]): R = testWith(())

  override def withBagReader[R](
    testWith: TestWith[BagReader[S3ObjectLocation, S3ObjectLocationPrefix], R]
  )(implicit context: Unit): R =
    testWith(new S3BagReader())

  override protected def toString(bucket: Bucket): String = bucket.name
}

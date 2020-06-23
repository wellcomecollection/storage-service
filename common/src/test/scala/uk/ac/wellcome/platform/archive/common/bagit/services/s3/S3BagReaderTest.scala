package uk.ac.wellcome.platform.archive.common.bagit.services.s3

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.bagit.services.{
  BagReader,
  BagReaderTestCases
}
import uk.ac.wellcome.platform.archive.common.fixtures.s3.S3BagBuilder
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.store.TypedStore
import uk.ac.wellcome.storage.store.s3.{NewS3StreamStore, NewS3TypedStore}
import uk.ac.wellcome.storage.{S3ObjectLocation, S3ObjectLocationPrefix}

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
  )(implicit context: Unit): R = {
    implicit val s3StreamStore: NewS3StreamStore = new NewS3StreamStore()

    testWith(new NewS3TypedStore[String]())
  }

  override def withNamespace[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  override def deleteFile(root: S3ObjectLocationPrefix, path: String)(
    implicit context: Unit
  ): Unit =
    deleteObject(root.asLocation(path))

  override def withContext[R](testWith: TestWith[Unit, R]): R = testWith(())

  override def withBagReader[R](
    testWith: TestWith[BagReader, R]
  )(implicit context: Unit): R =
    testWith(new S3BagReader())

  override protected def toString(bucket: Bucket): String = bucket.name
}

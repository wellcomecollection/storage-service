package weco.storage.services.s3

import weco.fixtures.TestWith
import weco.storage.fixtures.S3Fixtures
import weco.storage.fixtures.S3Fixtures.Bucket
import weco.storage.s3.S3ObjectLocation
import weco.storage.services.{RangedReader, RangedReaderTestCases}

class S3RangedReaderTest
    extends RangedReaderTestCases[S3ObjectLocation, Bucket]
    with S3Fixtures {
  override def withNamespace[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  override def createIdentWith(bucket: Bucket): S3ObjectLocation =
    createS3ObjectLocationWith(bucket)

  override def writeString(location: S3ObjectLocation, contents: String): Unit =
    putString(location, contents)

  override def withRangedReader[R](
    testWith: TestWith[RangedReader[S3ObjectLocation], R]
  ): R =
    testWith(new S3RangedReader())
}

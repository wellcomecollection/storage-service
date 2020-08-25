package uk.ac.wellcome.platform.archive.common.storage.services.s3

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.storage.services.{RangedReader, RangedReaderTestCases}
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.s3.S3ObjectLocation

class S3RangedReaderTest extends RangedReaderTestCases[S3ObjectLocation, Bucket] with S3Fixtures {
  override def withNamespace[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  override def createIdentWith(bucket: Bucket): S3ObjectLocation =
    createS3ObjectLocationWith(bucket)

  override def writeString(location: S3ObjectLocation, contents: String): Unit =
    s3Client.putObject(location.bucket, location.key, contents)

  override def withRangedReader[R](testWith: TestWith[RangedReader[S3ObjectLocation], R]): R =
    testWith(new S3RangedReader())
}

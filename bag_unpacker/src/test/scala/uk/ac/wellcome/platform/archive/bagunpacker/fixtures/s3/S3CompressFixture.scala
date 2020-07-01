package uk.ac.wellcome.platform.archive.bagunpacker.fixtures.s3

import uk.ac.wellcome.platform.archive.bagunpacker.fixtures.CompressFixture
import uk.ac.wellcome.storage.S3ObjectLocation
import uk.ac.wellcome.storage.fixtures.NewS3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket

trait S3CompressFixture
    extends CompressFixture[S3ObjectLocation, Bucket]
    with NewS3Fixtures {
  override def createLocationWith(
    bucket: Bucket,
    key: String
  ): S3ObjectLocation =
    createS3ObjectLocationWith(bucket, key = key)
}

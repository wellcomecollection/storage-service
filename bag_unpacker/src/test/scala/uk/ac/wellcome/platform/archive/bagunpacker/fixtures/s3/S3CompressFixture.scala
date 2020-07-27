package uk.ac.wellcome.platform.archive.bagunpacker.fixtures.s3

import uk.ac.wellcome.platform.archive.bagunpacker.fixtures.CompressFixture
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.s3.S3ObjectLocation

trait S3CompressFixture
    extends CompressFixture[S3ObjectLocation, Bucket]
    with S3Fixtures {
  override def createLocationWith(bucket: Bucket, key: String): S3ObjectLocation =
    S3ObjectLocation(bucket = bucket.name, key = key)
}

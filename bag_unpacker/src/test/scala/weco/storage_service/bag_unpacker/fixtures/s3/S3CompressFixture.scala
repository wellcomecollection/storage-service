package weco.storage_service.bag_unpacker.fixtures.s3

import weco.storage_service.bag_unpacker.fixtures.CompressFixture
import weco.storage.fixtures.S3Fixtures
import weco.storage.fixtures.S3Fixtures.Bucket
import weco.storage.s3.S3ObjectLocation

trait S3CompressFixture
    extends CompressFixture[S3ObjectLocation, Bucket]
    with S3Fixtures {
  override def createLocationWith(
    bucket: Bucket,
    key: String
  ): S3ObjectLocation =
    S3ObjectLocation(bucket = bucket.name, key = key)
}

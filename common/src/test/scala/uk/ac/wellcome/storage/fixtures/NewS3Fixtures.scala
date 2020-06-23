package uk.ac.wellcome.storage.fixtures

import uk.ac.wellcome.storage.{ObjectLocation, S3ObjectLocation}
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.streaming.InputStreamWithLength

trait NewS3Fixtures extends S3Fixtures {
  def createS3ObjectLocationWith(
    bucket: Bucket = createBucket
  ): S3ObjectLocation =
    S3ObjectLocation(
      bucket = bucket.name,
      key = randomAlphanumeric
    )

  def createS3ObjectLocation: S3ObjectLocation =
    createS3ObjectLocationWith()

  def putS3Object(
    location: S3ObjectLocation): Unit =
    putStream(
      location = ObjectLocation(namespace = location.bucket, path = location.key),
      inputStream = new InputStreamWithLength(randomInputStream(length = 20), length = 20)
    )

  def createInvalidBucket: Bucket =
    Bucket(createInvalidBucketName)
}

package uk.ac.wellcome.storage.fixtures

import uk.ac.wellcome.storage.S3ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket

trait NewS3Fixtures extends S3Fixtures {
  def createS3ObjectLocationWith(bucket: Bucket = createBucket): S3ObjectLocation =
    S3ObjectLocation(
      bucket = bucket.name,
      key = randomAlphanumeric
    )

  def createS3ObjectLocation: S3ObjectLocation =
    createS3ObjectLocationWith()
}

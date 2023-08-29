package weco.storage.listing.s3

import org.scalatest.Assertion
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.S3Object
import weco.storage.fixtures.S3Fixtures.Bucket
import weco.storage.providers.s3.S3ObjectLocation

class S3ObjectListingTest extends S3ListingTestCases[S3Object] {
  override def assertResultCorrect(
    result: Iterable[S3Object],
    entries: Seq[S3ObjectLocation])(implicit bucket: Bucket): Assertion = {
    val actualLocations =
      result.toSeq.map { s3Object =>
        S3ObjectLocation(bucket.name, key = s3Object.key())
      }

    actualLocations should contain theSameElementsAs entries
  }

  override def createS3Listing(implicit s3Client: S3Client = s3Client): S3Listing[S3Object] =
    new S3ObjectListing()
}

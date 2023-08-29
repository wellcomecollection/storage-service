package weco.storage.listing.s3

import org.scalatest.Assertion
import software.amazon.awssdk.services.s3.S3Client
import weco.storage.fixtures.S3Fixtures.Bucket
import weco.storage.providers.s3.S3ObjectLocation

class S3ObjectLocationListingTest extends S3ListingTestCases[S3ObjectLocation] {
  override def assertResultCorrect(result: Iterable[S3ObjectLocation],
                                   entries: Seq[S3ObjectLocation])(implicit bucket: Bucket): Assertion =
    result.toSeq should contain theSameElementsAs entries

  override def createS3Listing(implicit s3Client: S3Client = s3Client): S3Listing[S3ObjectLocation] =
    S3ObjectLocationListing()(s3Client)
}

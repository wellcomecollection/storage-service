package weco.storage.listing.s3

import software.amazon.awssdk.services.s3.S3Client
import weco.fixtures.TestWith
import weco.storage.fixtures.S3Fixtures
import weco.storage.fixtures.S3Fixtures.Bucket
import weco.storage.listing.fixtures.ListingFixtures
import weco.storage.providers.s3.{S3ObjectLocation, S3ObjectLocationPrefix}

trait S3ListingFixtures[ListingResult]
    extends S3Fixtures
    with ListingFixtures[
      S3ObjectLocation,
      S3ObjectLocationPrefix,
      ListingResult,
      S3Listing[ListingResult],
      Bucket] {
  def createIdent(implicit bucket: Bucket): S3ObjectLocation =
    createS3ObjectLocationWith(bucket)

  def extendIdent(location: S3ObjectLocation,
                  extension: String): S3ObjectLocation =
    location.join(extension)

  def createPrefix: S3ObjectLocationPrefix =
    createS3ObjectLocationPrefixWith(createBucket)

  def createPrefixMatching(location: S3ObjectLocation): S3ObjectLocationPrefix =
    location.asPrefix

  def withListingContext[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  def createS3Listing(implicit s3Client: S3Client = s3Client): S3Listing[ListingResult]
}

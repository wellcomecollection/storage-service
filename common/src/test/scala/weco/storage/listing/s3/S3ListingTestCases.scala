package weco.storage.listing.s3

import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.s3.model.NoSuchBucketException
import weco.fixtures.TestWith
import weco.storage.fixtures.S3Fixtures.Bucket
import weco.storage.listing.ListingTestCases
import weco.storage.providers.s3.{S3ObjectLocation, S3ObjectLocationPrefix}

trait S3ListingTestCases[ListingResult]
    extends ListingTestCases[
      S3ObjectLocation,
      S3ObjectLocationPrefix,
      ListingResult,
      S3Listing[ListingResult],
      Bucket]
    with S3ListingFixtures[ListingResult] {
  def withListing[R](bucket: Bucket, initialEntries: Seq[S3ObjectLocation])(
    testWith: TestWith[S3Listing[ListingResult], R]): R = {
    initialEntries
      .foreach { loc =>
        putStream(loc)
      }

    testWith(createS3Listing)
  }

  val listing: S3Listing[ListingResult] = createS3Listing

  describe("behaves as an S3 listing") {
    it("throws an exception if asked to list from a non-existent bucket") {
      val prefix = createPrefix

      val err = listing.list(prefix).left.value
      err.e.getMessage should startWith("The specified bucket does not exist")
      err.e shouldBe a[NoSuchBucketException]
    }

    it("ignores entries with a matching key in a different bucket") {
      withLocalS3Bucket { bucket =>
        val location = createS3ObjectLocationWith(bucket)
        putStream(location)

        // Now create the same keys but in a different bucket
        withLocalS3Bucket { queryBucket =>
          val queryLocation = location.copy(bucket = queryBucket.name)
          val prefix = queryLocation.asPrefix

          listing.list(prefix).value shouldBe empty
        }
      }
    }

    it("handles an error from S3") {
      val prefix = createPrefix

      val brokenListing = createS3Listing(s3Client = brokenS3Client)

      val err = brokenListing.list(prefix).left.value
      err.e.getMessage should startWith("Received an UnknownHostException when attempting to interact with a service")
      err.e shouldBe a[SdkClientException]
    }

    it("ignores objects in the same bucket with a different key") {
      withLocalS3Bucket { implicit bucket =>
        val location = createS3ObjectLocationWith(bucket)
        putStream(location)

        val prefix = location.join("subdir").asPrefix
        listing.list(prefix).value shouldBe empty
      }
    }

    it("fetches more objects than can be retrieved in a single page") {
      withLocalS3Bucket { implicit bucket =>
        val location = createS3ObjectLocationWith(bucket)

        // A single ListObjects API call can return up to 1000 objects
        // See https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html
        val locations = (1 to 1001).map { i =>
          location.join(s"file_$i.txt")
        }
        locations.foreach { loc =>
          putStream(loc)
        }

        val smallBatchListing = createS3Listing
        assertResultCorrect(
          smallBatchListing.list(location.asPrefix).value,
          entries = locations
        )
      }
    }
  }
}

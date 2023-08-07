package weco.storage.listing.s3

import software.amazon.awssdk.services.s3.S3Client
import weco.storage.providers.s3.{S3ObjectLocation, S3ObjectLocationPrefix}

class S3ObjectLocationListing(implicit objectListing: S3ObjectListing)
    extends S3Listing[S3ObjectLocation] {
  override def list(prefix: S3ObjectLocationPrefix): ListingResult =
    objectListing.list(prefix) match {
      case Right(result) =>
        Right(result.map { s3Object =>
          S3ObjectLocation(bucket = prefix.bucket, key = s3Object.key())
        })
      case Left(err) => Left(err)
    }
}

object S3ObjectLocationListing {
  def apply()(implicit s3Client: S3Client): S3ObjectLocationListing = {
    implicit val summaryListing: S3ObjectListing =
      new S3ObjectListing()

    new S3ObjectLocationListing()
  }
}

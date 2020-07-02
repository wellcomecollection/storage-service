package uk.ac.wellcome.storage.listing.s3

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.storage.{ListingFailure, S3ObjectLocation, S3ObjectLocationPrefix}

class NewS3ObjectLocationListing(implicit s3Client: AmazonS3) extends NewS3Listing[S3ObjectLocation] {
  private val underlying: S3ObjectLocationListing =
    S3ObjectLocationListing()

  override def list(prefix: S3ObjectLocationPrefix): ListingResult =
    underlying.list(prefix.toObjectLocationPrefix) match {
      case Right(iterable)              => Right(iterable.map { loc => S3ObjectLocation(loc) })
      case Left(ListingFailure(_, err)) => Left(ListingFailure(prefix, err))
    }
}

package uk.ac.wellcome.storage.transfer.s3

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.storage.listing.s3.NewS3ObjectLocationListing
import uk.ac.wellcome.storage.transfer.NewPrefixTransfer
import uk.ac.wellcome.storage.{S3ObjectLocation, S3ObjectLocationPrefix}

class NewS3PrefixTransfer()(
  implicit val transfer: NewS3Transfer,
  val listing: NewS3ObjectLocationListing
) extends NewPrefixTransfer[
      S3ObjectLocation,
      S3ObjectLocationPrefix,
      S3ObjectLocation,
      S3ObjectLocationPrefix
    ] {
  override protected def buildDstLocation(
    srcPrefix: S3ObjectLocationPrefix,
    dstPrefix: S3ObjectLocationPrefix,
    srcLocation: S3ObjectLocation
  ): S3ObjectLocation =
    dstPrefix.asLocation(
      srcLocation.key.stripPrefix(srcPrefix.keyPrefix)
    )
}

object NewS3PrefixTransfer {
  def apply()(implicit s3Client: AmazonS3): NewS3PrefixTransfer = {
    implicit val transfer: NewS3Transfer = new NewS3Transfer()
    implicit val listing: NewS3ObjectLocationListing =
      new NewS3ObjectLocationListing()

    new NewS3PrefixTransfer()
  }
}

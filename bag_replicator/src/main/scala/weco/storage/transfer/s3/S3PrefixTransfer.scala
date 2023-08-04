package weco.storage.transfer.s3

import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.transfer.s3.S3TransferManager
import weco.storage.listing.s3.S3ObjectLocationListing
import weco.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import weco.storage.transfer.PrefixTransfer

class S3PrefixTransfer()(
  implicit val transfer: S3Transfer,
  val listing: S3ObjectLocationListing
) extends PrefixTransfer[
      S3ObjectLocationPrefix,
      S3ObjectLocation,
      S3ObjectLocationPrefix,
      S3ObjectLocation,
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

object S3PrefixTransfer {
  def apply()(implicit s3Client: S3Client,
              transferManager: S3TransferManager): S3PrefixTransfer = {
    implicit val transfer: S3Transfer = S3Transfer.apply()
    implicit val listing: S3ObjectLocationListing = S3ObjectLocationListing()

    new S3PrefixTransfer()
  }
}

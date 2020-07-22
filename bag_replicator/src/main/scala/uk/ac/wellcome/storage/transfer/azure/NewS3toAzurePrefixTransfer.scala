package uk.ac.wellcome.storage.transfer.azure

import uk.ac.wellcome.storage.listing.s3.NewS3ObjectLocationListing
import uk.ac.wellcome.storage.transfer.NewPrefixTransfer
import uk.ac.wellcome.storage.{AzureBlobItemLocation, AzureBlobItemLocationPrefix, S3ObjectLocation, S3ObjectLocationPrefix}

class NewS3toAzurePrefixTransfer()(
  implicit val transfer: NewS3toAzureTransfer,
  val listing: NewS3ObjectLocationListing
) extends NewPrefixTransfer[
  S3ObjectLocation,
  S3ObjectLocationPrefix,
  AzureBlobItemLocation,
  AzureBlobItemLocationPrefix] {
  override protected def buildDstLocation(
                                           srcPrefix: S3ObjectLocationPrefix,
                                           dstPrefix: AzureBlobItemLocationPrefix,
                                           srcLocation: S3ObjectLocation
                                         ): AzureBlobItemLocation =
    dstPrefix.asLocation(
      srcLocation.key.stripPrefix(srcPrefix.keyPrefix)
    )
}
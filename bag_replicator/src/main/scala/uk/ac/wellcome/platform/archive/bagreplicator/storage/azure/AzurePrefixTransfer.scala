package uk.ac.wellcome.platform.archive.bagreplicator.storage.azure

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.S3ObjectSummary
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import uk.ac.wellcome.storage.listing.Listing
import uk.ac.wellcome.storage.listing.s3.S3ObjectSummaryListing
import uk.ac.wellcome.storage.s3.S3ObjectLocationPrefix
import uk.ac.wellcome.storage.transfer.PrefixTransfer

class AzurePrefixTransfer(
  implicit
  s3Client: AmazonS3,
  val transfer: AzureTransfer[_]
) extends PrefixTransfer[
      S3ObjectLocationPrefix,
      S3ObjectSummary,
      AzureBlobLocationPrefix,
      AzureBlobLocation
    ] {

  override implicit val listing
    : Listing[S3ObjectLocationPrefix, S3ObjectSummary] =
    new S3ObjectSummaryListing()

  override protected def buildDstLocation(
    srcPrefix: S3ObjectLocationPrefix,
    dstPrefix: AzureBlobLocationPrefix,
    srcSummary: S3ObjectSummary
  ): AzureBlobLocation =
    dstPrefix.asLocation(
      srcSummary.getKey.stripPrefix(srcPrefix.keyPrefix)
    )
}

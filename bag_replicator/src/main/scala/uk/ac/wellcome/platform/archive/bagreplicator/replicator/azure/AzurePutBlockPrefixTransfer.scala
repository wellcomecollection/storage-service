package uk.ac.wellcome.platform.archive.bagreplicator.replicator.azure

import com.amazonaws.services.s3.AmazonS3
import com.azure.storage.blob.BlobServiceClient
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import uk.ac.wellcome.storage.listing.Listing
import uk.ac.wellcome.storage.listing.s3.S3ObjectLocationListing
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.storage.transfer.{PrefixTransfer, Transfer}

class AzurePutBlockPrefixTransfer(
  implicit
  s3Client: AmazonS3,
  blobServiceClient: BlobServiceClient
) extends
  PrefixTransfer[
    S3ObjectLocationPrefix,
    S3ObjectLocation,
    AzureBlobLocationPrefix,
    AzureBlobLocation
  ] {

  override protected def buildDstLocation(
    srcPrefix: S3ObjectLocationPrefix,
    dstPrefix: AzureBlobLocationPrefix,
    srcLocation: S3ObjectLocation
  ): AzureBlobLocation =
    dstPrefix.asLocation(
      srcLocation.key.stripPrefix(srcPrefix.keyPrefix)
    )

  override implicit val transfer: Transfer[S3ObjectLocation, AzureBlobLocation] =
    new AzurePutBlockTransfer()

  override implicit val listing: Listing[S3ObjectLocationPrefix, S3ObjectLocation] =
    S3ObjectLocationListing()
}

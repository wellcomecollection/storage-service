package uk.ac.wellcome.platform.archive.bagreplicator.replicator.azure

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.azure.storage.blob.BlobServiceClient
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.Replicator
import uk.ac.wellcome.platform.archive.bagreplicator.storage.azure.{
  AzurePrefixTransfer,
  AzureTransfer
}
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import uk.ac.wellcome.storage.listing.azure.AzureBlobLocationListing

class AzureReplicator(
  transfer: AzureTransfer[_]
)(
  implicit s3Client: AmazonS3,
  blobClient: BlobServiceClient
) extends Replicator[
      S3ObjectSummary,
      AzureBlobLocation,
      AzureBlobLocationPrefix
    ] {

  override implicit val dstListing: AzureBlobLocationListing =
    AzureBlobLocationListing()

  override implicit val prefixTransfer: AzurePrefixTransfer =
    new AzurePrefixTransfer()(s3Client, transfer)

  override protected def buildDestinationFromParts(
    container: String,
    namePrefix: String
  ): AzureBlobLocationPrefix =
    AzureBlobLocationPrefix(container = container, namePrefix = namePrefix)
}

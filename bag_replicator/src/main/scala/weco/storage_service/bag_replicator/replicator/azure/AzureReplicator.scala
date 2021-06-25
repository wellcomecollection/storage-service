package weco.storage_service.bag_replicator.replicator.azure

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.azure.storage.blob.BlobServiceClient
import weco.storage_service.bag_replicator.replicator.Replicator
import weco.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import weco.storage.listing.azure.AzureBlobLocationListing
import weco.storage.transfer.azure.{
  AzurePrefixTransfer,
  AzureTransfer
}

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
